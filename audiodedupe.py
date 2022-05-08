#!/usr/bin/env python

from ast import arg
from fileinput import filename
import os
import re
import sys
import json
import shutil
import subprocess
import argparse
import multiprocessing as mp

DEFAULT_CACHE_ENABLED = True
DEFAULT_CACHE_DIR = os.path.join(
    os.path.expanduser('~'), '.cache', 'audiodedupe')
DEFAULT_CACHE_FILE_NAME = 'audiodedupe_cache.json'
DEFAULT_FILES_FILTER = '(?i)^.*\.(mp3|ogg|wav)$'
DEFAULT_FINGERPRINT_CMD = 'fpcalc'
DEFAULT_FINGERPRINT_CMD_ARGS = ['-json']
DEFAULT_FINGERPRINT_CMD_TIMEOUT = 10
DEFAULT_CONCURRENT_PROCESSES = mp.cpu_count()


class AudioDedupeException(Exception):
    pass


class AudioDedupe:
    def __init__(self,
                 cacheEnabled=DEFAULT_CACHE_ENABLED,
                 cacheDir=DEFAULT_CACHE_DIR,
                 filesFilter=DEFAULT_FILES_FILTER,
                 fingerprintCmd=DEFAULT_FINGERPRINT_CMD,
                 fingerprintCmdArgs=DEFAULT_FINGERPRINT_CMD_ARGS,
                 fingerprintCmdTimeout=DEFAULT_FINGERPRINT_CMD_TIMEOUT,
                 concurrentProcesses=DEFAULT_CONCURRENT_PROCESSES):
        self.cacheEnabled = cacheEnabled
        self.cacheDir = cacheDir
        self.cacheFile = os.path.join(self.cacheDir, DEFAULT_CACHE_FILE_NAME)
        self.filesFilter = filesFilter
        self.fingerprintCmd = fingerprintCmd
        self.fingerprintCmdArgs = fingerprintCmdArgs
        self.fingerprintCmdTimeout = fingerprintCmdTimeout
        self.concurrentProcesses = concurrentProcesses
        self.fingerprints = {}
        self.reverseFingerprints = {}
        if not self._fingerprintCmdExists():
            raise AudioDedupeException('fingerprint command not found')
        if cacheEnabled:
            self.loadCache()
        self._updateReverseFingerprints()

    def _fingerprintCmdExists(self):
        return shutil.which(self.fingerprintCmd) is not None

    def loadCache(self):
        if not os.path.isfile(self.cacheFile):
            return False
        try:
            with open(self.cacheFile, 'r') as fd:
                self.fingerprints = json.load(fd)
        except Exception as ex:
            os.remove(self.cacheFile)
        return True

    def writeCache(self):
        if not os.path.isdir(self.cacheDir):
            os.makedirs(self.cacheDir)
        with open(self.cacheFile, 'w') as fd:
            json.dump(self.fingerprints, fd)
        return True

    def _updateReverseFingerprints(self):
        for fingerprint, files in self.fingerprints.items():
            for fileName in files:
                self.reverseFingerprints[fileName] = fingerprint

    def scanFile(self, fileName):
        if fileName in self.reverseFingerprints:
            return {
                'file': fileName,
                'fingerprint': self.reverseFingerprints[fileName],
                'success': True
            }
        while True:
            try:
                if fileName is None:
                    break
                with subprocess.Popen([self.fingerprintCmd] + self.fingerprintCmdArgs + [fileName],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
                    out, err = proc.communicate(
                        timeout=self.fingerprintCmdTimeout)
                    if proc.returncode != 0:
                        return None
                    jsonOut = json.loads(out)
                    if 'fingerprint' not in jsonOut:
                        return None
                    jsonOut['file'] = fileName
                    jsonOut['success'] = True
                    self.reverseFingerprints[fileName] = jsonOut['fingerprint']
                    return jsonOut
            except Exception as e:
                return None
        return None

    def _descend(self, directory):
        directory = os.path.abspath(directory)
        reFilter = re.compile(self.filesFilter)
        for dirName, subdirList, fileList in os.walk(directory):
            for fileName in fileList:
                if not reFilter.match(fileName):
                    continue
                fullPath = os.path.join(directory, dirName, fileName)
                yield fullPath

    def scan(self, directory):
        with mp.Pool(processes=self.concurrentProcesses) as pool:
            results = pool.map(self.scanFile, self._descend(directory))
            for res in results:
                if not res:
                    continue
                if not res.get('success'):
                    continue
                fileName = res.get('file')
                fingerprint = res.get('fingerprint')
                if not (fileName and fingerprint):
                    continue
                self.fingerprints.setdefault(fingerprint, [])
                if fileName not in self.fingerprints[fingerprint]:
                    self.fingerprints[fingerprint].append(fileName)
        if self.cacheEnabled:
            self.writeCache()

    def pruneFingerprints(self):
        for fingerprint, files in self.fingerprints.items():
            for fileName in files:
                if not os.path.isfile(fileName):
                    files.remove(fileName)
                    if fileName in self.reverseFingerprints:
                        del self.reverseFingerprints[fileName]
            if not files:
                del self.fingerprints[fingerprint]
        if self.cacheEnabled:
            self.writeCache()

    def analyzeResults(self):
        self.pruneFingerprints()
        print('Found %d duplicated songs' % len(
            [fp for fp in self.fingerprints if len(self.fingerprints[fp]) > 1]))
        idx = 1
        for fingerprint, files in self.fingerprints.items():
            if len(files) > 1:
                print('#%d:' % idx)
                for fileName in files:
                    print('%s' % fileName)
                idx += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Find duplicated audio files.')
    parser.add_argument('dirs', metavar='DIR', nargs='+',
                        help='directories to look for audio files')
    parser.add_argument('----files-filter', default=DEFAULT_FILES_FILTER,
                        help='regexp used to filter file to analyze (default: %s)' % DEFAULT_FILES_FILTER)
    parser.add_argument('--disable-cache', action='store_false',
                        help='disable the cache (default: False)')
    parser.add_argument('--cache-dir', default=DEFAULT_CACHE_DIR,
                        help='cache directory (default: %s)' % DEFAULT_CACHE_DIR)
    parser.add_argument('--reset-cache', action='store_true',
                        help='delete the cache file before proceeding')
    parser.add_argument('--fingerprint-cmd', default=DEFAULT_FINGERPRINT_CMD,
                        help='command to calculate the finterprint (default: %s)' % DEFAULT_FINGERPRINT_CMD)
    parser.add_argument('--fingerprint-cmd-args', action='append', default=DEFAULT_FINGERPRINT_CMD_ARGS,
                        help='arguments of finterprint command (default: %s)' % ' '.join(DEFAULT_FINGERPRINT_CMD_ARGS))
    parser.add_argument('--fingerprint-cmd-timeout', type=int, default=DEFAULT_FINGERPRINT_CMD_TIMEOUT,
                        help='timeout of the fingerprint command (default: %d seconds)' % DEFAULT_FINGERPRINT_CMD_TIMEOUT)
    parser.add_argument('--concurrent-processes', type=int, default=DEFAULT_CONCURRENT_PROCESSES,
                        help='number of concurrent fingerprint processes to spawn (default: %d)' % DEFAULT_CONCURRENT_PROCESSES)
    args = parser.parse_args()
    adArgs = {}
    adArgs['cacheEnabled'] = args.disable_cache  # flag is a negation
    adArgs['cacheDir'] = args.cache_dir
    adArgs['filesFilter'] = args.files_filter
    adArgs['fingerprintCmd'] = args.fingerprint_cmd
    adArgs['fingerprintCmdArgs'] = args.fingerprint_cmd_args
    adArgs['fingerprintCmdTimeout'] = args.fingerprint_cmd_timeout
    adArgs['concurrentProcesses'] = args.concurrent_processes
    if args.reset_cache:
        cacheFile = os.path.join(args.cache_dir, DEFAULT_CACHE_FILE_NAME)
        if os.path.isfile(cacheFile):
            os.remove(cacheFile)
    try:
        audiodedupe = AudioDedupe(**adArgs)
    except AudioDedupeException as ex:
        print('please make sure that you have the %s executable installed' %
              args.fingerprint_cmd)
        sys.exit(1)
    for directory in args.dirs:
        audiodedupe.scan(directory)
    audiodedupe.analyzeResults()

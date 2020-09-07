import time
import random
import os
import psutil
import logging
import sys

# Libraries that must be installed:
import requests

# Custom libraries:
from include import globals

def elapsed(timestamp, precision=1):
    ''' Return how many seconds have elapsed since provided timestamp, with optional decimal precision. '''
    return round(time.time() - timestamp, precision)

def vprint(message, level=1):
    ''' Optional verbose output. '''
    if globals.args.verbose and globals.args.verbose >= level:
        # If running with concurrency, prepend PID.
        try:
            # Concurrency may not be defined by all scripts
            if globals.args.concurrency and globals.args.concurrency > 1:
                print("[%d] [%.3f] %s" % (os.getpid(), elapsed(globals.start, 3), message))
                return
        except:
            pass
        print("[%.3f] %s " % (elapsed(globals.start, 3), message))

def supported_coins(settings):
    return [type for type in settings.coins.keys()]

def request_chaininfo(settings):
    retry_limit = 10
    for loop_counter in range(retry_limit):
        try:
            if globals.args.host:
                url = "http://%s/rest/chaininfo.json" % (globals.args.host,)
            else:
                url = "http://%s/rest/chaininfo.json" % (settings.coins[globals.args.type]['server'],)
            vprint("requesting %s" % (url,), level=4)
            response = requests.get(url)
            if response.status_code == 200:
                vprint("success (200)", level=4)
                return response.json()
            else:
                vprint("REST request failed with status code %d" % (response.status_code,))
                if response.status_code == 404:
                    vprint("path not found (404): be sure daemon was started with -rest flag")
                elif response.status_code == 503:
                    vprint("server error (404): daemon may still be starting, try again shortly")
        except Exception as e:
            if loop_counter < (retry_limit - 1):
                # Add a random delay to avoid lock-stepping when running with concurrency.
                sleep_for = 10 * loop_counter * random.randint(1, 3)
                vprint("REST request [%d] for block %s failed (retrying in %.1f seconds): %s" % (loop_counter, hash, sleep_for, e))
                time.sleep(sleep_for)
            else:
                vprint("REST request [%d] for block %s failed, too many failures, giving up: %s" % (loop_counter, hash, e))
                print(response)
                return None

def request_block(hash, settings):
    retry_limit = 10
    for loop_counter in range(retry_limit):
        try:
            if globals.args.host:
                url = "http://%s/rest/block/%s.json" % (globals.args.host, hash)
            else:
                url = "http://%s/rest/block/%s.json" % (settings.coins[globals.args.type]['server'], hash)
            vprint("requesting %s" % (url,), level=4)
            response = requests.get(url)
            if response.status_code == 200:
                vprint("success (200)", level=4)
                return response.json()
            else:
                vprint("REST request failed with status code %d" % (response.status_code,))
                if response.status_code == 404:
                    vprint("path not found (404): be sure daemon was started with -rest flag")
                elif response.status_code == 503:
                    vprint("server error (404): daemon may still be starting, try again shortly")
                return None
        except Exception as e:
            if loop_counter < (retry_limit - 1):
                # Add a random delay to avoid lock-stepping when running with concurrency.
                sleep_for = 10 * loop_counter * random.randint(1, 3)
                vprint("REST request [%d] for block %s failed (retrying in %.1f seconds): %s" % (loop_counter, hash, sleep_for, e))
                time.sleep(sleep_for)
            else:
                vprint("REST request [%d] for block %s failed, too many failures, giving up: %s" % (loop_counter, hash, e))
                print(response)
                return None

def working_path():
    if globals.args.working:
        return globals.args.working + "/blockchain_data/" + globals.args.type + "/"
    else:
        return os.getcwd() + "/blockchain_data/" + globals.args.type + "/"

def gzipped_sort(source_file, destination_file, lines):
    sort_start = time.time()
    vprint("sorting %s" % (source_file,))

    # Allow overriding sort command in settings file (primary to adjust memory usage or sort destination)
    try:
        system_sort_command = globals.settings.system_sort_command.replace('{coin}', globals.args.type)
        vprint("loaded sort command from settings: '%s'" % system_sort_command)
    except:
        system_sort_command = \
            "gzip -dc %s | LANG=C sort -u -S 8G -T /tmp/%s --compress-program=gzip | pv -l -s %d | gzip > %s" \
            % globals.args.type
        vprint("using default sort command: '%s'" % system_sort_command)

    os.system(system_sort_command % (source_file, lines, destination_file))
    vprint("sort completed in %s seconds" % (elapsed(sort_start, )))

# From https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def human_readable(num, suffix='B'):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)

def memory_snapshot(message="memory snapshot"):
    if globals.snapshot_memory:
        memory = psutil.Process(os.getpid()).memory_info()
        logging.info("%s(%s): rss=%s vms=%s shared=%s, text=%s, lib=%s, data=%s, dirty=%s" %
                     (message, sys._getframe().f_back.f_code.co_name, human_readable(memory.rss),
                      human_readable(memory.vms), human_readable(memory.shared), human_readable(memory.text),
                      human_readable(memory.lib), human_readable(memory.data), human_readable(memory.dirty)))

def get_debug_level():
    try:
        debug_level = int(globals.settings.debug)
    except:
        # By default we enable minimal debugging
        debug_level = 1

    return debug_level

def debug(message=None, level=1):
    debug_level = get_debug_level()

    if debug_level <= 0:
        return None

    # If enabled, store debug info in globals.debug
    if message:
        if level <= debug_level:
            globals.debug.append(message)
    else:
        return globals.debug


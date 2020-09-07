#!/bin/bash

BASEDIR="/app"

COIN=$1
INITIALIZE=$2
WORKDIR="${BASEDIR}/blockchain_data/${COIN}/"
MASTERLOCK="${WORKDIR}/extract.lock"
WAITLOCK="${WORKDIR}/wait.lock"

function get_lock() {
  if [ $1 == "master" ]; then
    LOCK=${MASTERLOCK}
  elif [ $1 == "wait" ]; then
    LOCK=${WAITLOCK}
  fi

  # Check if running process already has lock
  if [ -e ${LOCK} ] && kill -0 `cat ${LOCK}`; then
    return 0
  fi

  # Remove lockfile when we exit
  trap "rm -f ${LOCK}; exit" INT TERM EXIT
  # Claim the lock
  echo $$ > ${LOCK}
  echo "got ${1} lock"
  return 1
}

function extract_blockchain() {
  # Extract blockchain.
  cd $BASEDIR
  if [ -z "${INITIALIZE}" ] || [ "${INITIALIZE}" != "--initialize" ]; then
      echo "extracting..."
      python3 extract.py -v -t ${COIN} --cleanup --limit 25000 2>&1 | tee ${WORKDIR}/extract-debug.log
  else
      echo "extracting...starting with genesis block"
      python3 extract.py -v -t ${COIN} --initial --cleanup 2>&1 | tee ${WORKDIR}/extract-debug.log
  fi
}

function reload_gunicorn() {
  ps auxww | grep gunicorn | head -1 | awk '{print $2}' | xargs kill -HUP
}

function release_lock() {
  if [ $1 == "master" ]; then
    echo "released master lock"
    rm -f ${MASTERLOCK}
  elif [ $1 == "wait" ]; then
    echo "released wait lock"
    rm -f ${WAITLOCK}
  fi
}

function wait_for_master_lock() {
  echo "waiting for master lock"
  LOOP=1
  while [ $LOOP -gt 0 ]; do
    get_lock "master"
    HAVE_LOCK=$?
    if [ ${HAVE_LOCK} -eq 1 ]; then
      return 1
    fi
    sleep 5
  done
}

if [ $# -lt 1 ]; then
  echo "You must specify coin type."
  exit 1
fi


# Try and grab the master lock.
get_lock "master"
HAVE_LOCK=$?

# We did not get the master lock, try and grab the wait lock.
if [ ${HAVE_LOCK} != 1 ]; then
  get_lock "wait"
  HAVE_LOCK=$?
  # We did not get the wait lock, exit, we're done.
  if [ ${HAVE_LOCK} == 0 ]; then
    exit
  else
    # We have the wait lock, now wait until we get the master lock.
    wait_for_master_lock
    # Got it, release the wait lock.
    release_lock "wait"
  fi
fi

# We have the master lock, extract the blockchain.
extract_blockchain
#reload_gunicorn
release_lock "master"

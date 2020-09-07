function get_lock() {
  # Check if still running from the last time this was invoked.
  if [ -e ${LOCKFILE} ] && kill -0 `cat ${LOCKFILE}`; then
    echo "Exiting, already running"
    exit
  fi

  # Make sure the lockfile is removed when we exit and then claim it.
  trap "rm -f ${LOCKFILE}; exit" INT TERM EXIT
  echo $$ > ${LOCKFILE}
}

function extract_coin() {
  # Extract blockchain.
  cd $WORKDIR
  . .venv/bin/activate
  python extract.py -v -t $COIN -l $LIMIT > /tmp/extract_$COIN.log 2>&1
}

function release_lock() {
  rm -f ${LOCKFILE}
}

function notify() {
  cat /tmp/extract_$COIN.log | mailx -s "$COIN batch completed" jeremy@amailbox.net
}

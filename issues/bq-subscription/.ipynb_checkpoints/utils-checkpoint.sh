[[ $1 == "off" ]] && sed 's/$LOGREPLACE/ >>logs/test.log 2>\&1/' dev.sh > rc.sh



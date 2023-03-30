FNAME="./test_data/input_data.txt"
RES_FNAME="./test_data/result_data.txt"
RES_TFNAME="./test_data/result_data_test.txt"

COUNT_TEST_ARRAY=(1 100 1000 10000 100000 1000000 10000000)
N_TEST_ARRAY=(10000 100000 1000000 10000000) #



for count in ${COUNT_TEST_ARRAY[@]}; do
        if test -f "$FNAME"; then
            rm $FNAME
        fi

        touch $FNAME
        echo ""
        echo `date +"%T"` "prepare $FNAME with $count elements"
        LC_ALL=C tr -dc 'a-z ' < /dev/urandom | fold -s -w 10 | head -n $count > $FNAME

        echo `date +"%T"` "sort to $RES_TFNAME"

        cat $FNAME | sort | uniq -c | sed 's/^ *\([0-9][0-9]*\) /\1\t/' | awk  -F'\t' '{print $2 "\t" $1}' > $RES_TFNAME

        for n in ${N_TEST_ARRAY[@]}; do
                echo ""
                echo `date +"%T"` "counting keys for n = " $n

                ./key_counter -n $n -i $FNAME -o $RES_FNAME

                echo `date +"%T"` "compare $RES_TFNAME and  $RES_FNAME" 
                DIFF=$(diff  $RES_TFNAME $RES_FNAME) 
                if [ "$DIFF" == "" ] 
                then
                    echo `date +"%T"` "compare $RES_TFNAME and  $RES_FNAME   OK"
                fi


        done
done

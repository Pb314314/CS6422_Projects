#!/usr/bin/env bash

function verify() {
	arr=("$@")
	for i in "${arr[@]}";
		do
				if [ ! -f $i ]; then

					echo "Missing ${i}"
					exit 1
				fi
		done
}
# req_files=("src/tutorial/tutorial.cc" "src/include/tutorial/tutorial.h" )

req_files=("src/tutorial/tutorial.cc" "src/include/tutorial/tutorial.h" "src/tutorial/trie.cc" "src/include/tutorial/trie.h")
verify "${req_files[@]}"	
if [[ $? -ne 0 ]]; then
    exit 1
fi

if [ $# -eq 1 ]
then
	#zip "${1}.zip" src/tutorial/tutorial.cc src/include/tutorial/tutorial.h REPORT.md
	zip "${1}.zip" src/tutorial/tutorial.cc src/include/tutorial/tutorial.h src/tutorial/trie.cc src/include/tutorial/trie.h REPORT.md
	
else
	echo 'Please provide a file name, eg ./submit Gaurav'
fi

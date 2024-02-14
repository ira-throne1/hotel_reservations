How to run the program:
1) run
./main.sh
in bash

2) the script will prompt to input the location of the customer-reservations.csv in
hdfs:///foldername
format (without the last backslash)

3) then the script will prompt to input the location of the hotel-booking.csv file in the same
hdfs:///foldername
format

4) finally, it will prompt to enter the location for the output filr in
/foldername
format

5) once the script is done, the path to the output file will be written in third-to-last line of the log in the form of
job output is in hdfs:///user/hadoop/utput=/foldername



# C_cassandra
Using Cassandra within a C file.

# Install
- Since I'm using ubuntu 20.04+, I built my own DataStax C/C++ Driver following this tutorial :
https://docs.datastax.com/en/developer/cpp-driver/2.16/topics/building/
- Also need Cassandra and java.

# Features :
- Creating a data table, and inserting some datas in it.
- Printing those datas to the terminal.
- Selecting some datas with a WHERE clause.

# Screenshot : 
- I haven't find a way to print the miliseconds. At the end, it is totally normal to see only one row from the SELECT since the time is 10:00:00:001.
![alt text](https://github.com/Clement-Devevey/C_cassandra/blob/master/screen/screens.png?raw=true)

## Solution to Data Engineering Fellowship coding challenge questions
This is my solution for the Insight Data Engineering Fellowship Challenge Questions.


For simplicity I have hard-coded the output files to the main program as they are unlikely to change.
The input file can however be passed as a commandline argument.
Also I avoided using Pandas to read the data as it tends not to do well with large data. Instead to extract the tweets and hashtags I resulted in using generators and python data structures for speed and easy of reading and writing.


- Usage: python solution.py path_to_input_file

## Dependencies:
The following libraries have been used to solve this problem.
```
1. unicodedata
2. re
3. string
4. sys
5. os
6. simplejson
7. itertools
8. time
9. itertools
10. collections
11. datetime

```

For external libraries, please install using the following command

```
pip install "library_name"
```

## Personal Library
1. graph (In helper_module directory)


File Tree Structure
```
.
├── README.md
├── README.md~
├── data-gen
│   ├── README.md
│   └── get-tweets.py
├── images
│   ├── directory-pic.png
│   ├── htag_degree_1.png
│   ├── htag_degree_2.png
│   ├── htag_degree_3.png
│   ├── htag_degree_4.png
│   ├── htag_degree_5.png
│   ├── htag_graph_1.png
│   ├── htag_graph_2.png
│   ├── htag_graph_3.png
│   ├── htag_graph_4.png
│   └── htag_graph_5.png
├── run.sh
├── src
│   ├── helper_modules
│   │   ├── __init__.py
│   │   └── graph.py
│   └── solution.py
├── tweet_input
│   └── tweets.txt
└── tweet_output
    ├── ft1.txt
    └── ft2.txt

6 directories, 22 files

```
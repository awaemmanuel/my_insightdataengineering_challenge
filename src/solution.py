#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# Standard Library Importations
import unicodedata as ud, re, string, sys, os, simplejson, itertools, time
from itertools import islice, chain
from collections import OrderedDict, deque
from datetime import datetime, timedelta

# Personal Libraries import
from helper_modules import graph


######### HELPER CLASSES #################
def constant(f):
    def fset(self, value):
        raise TypeError
    def fget(self):
        return f()
    return property(fget, fset)

class _Const(object):
    @constant
    def HASH_GRAPH_UPDATE_INTERVAL():
        return 59


############## HELPER FUNCTIONS ####################

'''
    Print to Screen
'''
def print_out(str):
    print str
    sys.stdout.flush()

'''
    Terminal spinning cursor simulator
'''
def spinning_cursor():
    spinner = itertools.cycle(['-', '/', '|', '\\'])
    for _ in range(50):
        sys.stdout.write(spinner.next())
        sys.stdout.flush()
        time.sleep(0.3)
        sys.stdout.write('\b')

'''
    Function to convert string internally to unicode
'''
def to_unicode(unicode_or_str):
    if isinstance(unicode_or_str, str):
        value = unicode_or_str.decode('utf-8')
    else:
        value = unicode_or_str
    return value

'''
    Function to convert unicode back to string for output
'''

def to_str(unicode_or_str):
    if isinstance(unicode_or_str, unicode):
        value = unicode_or_str.encode('utf-8')
    else:
        value = unicode_or_str
    return value

'''
    Function that strips unicode with REGEX
'''

def strip_unicode(text):
    #REGEX for tweet unicode removal
    str_text = to_str(text)
    #str_text = ud.normalize('NFKD', text).encode('ascii','ignore') # Incase we want to 
    str_text =  re.sub(r'(\\u[0-9A-Fa-f]+)', '', str_text)
    return to_unicode(str_text)


'''
    Function returns TRUE if pattern exists in text
'''

def contains_pattern(text, pattern):
    return True if (re.search(pattern, text)) else False
    
    
'''
    Function that checks if text is empty
'''

def is_empty(text):
    return not text.strip()


'''
    Function that checks if text contains only punctuations
'''

def is_not_only_punc(text):
    pattern = re.compile("[{}]".format(re.escape(string.punctuation)))
    text_list = text.split()
    return True if [char for char in text_list if not pattern.match(char)] else False
    
    
'''
    Function that guarantees we process only basic latin characters
'''

def remove_non_basic_latin_chars(text):
    # Process only basic latin characters else continue
    return ''.join([c for c in text if ord(c) in xrange(32, 128)])


############## PROCESS FLOW FUNCTIONS ####################

'''
    Generate tweets text and timestamp 
    
    Yields a single tweet timestamp and text on at a time using a generator,
    which must be properly handled by the generator caller.
'''

def extract_tweet_text_and_timestamp(input_file):
    
    # Open input file, generate text and timestamp
    try:
        with open(input_file) as twitter_input:
            for line in twitter_input:
                if not line: # To filter out keep-alive new lines
                    continue
                single_tweet = simplejson.loads(line)
                if 'text' in single_tweet:
                    
                    text = single_tweet['text']
                    time_stamp = single_tweet['created_at']
                    
                    yield text, time_stamp
    
    except IOError:
        sys.stderr.write("[extract_tweet_text_and_timestamp] - Error: Could not open {}\n".format(input_file))
        sys.exit(-1)


'''
    clean_tweet remove unicode and track number of these tweets
    :type str: text
    :rtype str
'''
def clean_string(text):
    
    if not text: return ("", False)
        
    text_has_unicode = False
    
    # Convert text to raw literals and unicode to avoid regex  and internal processing issues.
    text = text.replace("\\", "\\\\")
    text = to_unicode(text)
    
    # Unicode pattern
    unicode_pattern = re.compile(u'(\\u[0-9A-Fa-f]+)')
    
    # Keep track of texts with unicode and then remove the unicode chars
    if contains_pattern(text, unicode_pattern):
        text_has_unicode = True
        text = strip_unicode(text)

    # Remove escape characters. This is faster than using re.compile(r'\s+') class
    text = ' '.join(text.strip().split())
    text = text.replace("\\", "")
    
    # Return clean/normalized text and also if a unicode character was found.
    return to_str(text), text_has_unicode


'''
    Function that processes a text message and writes to a text file in required output
'''

def process_tweets(input_file, output_file):       
    
    # check if file exists on filesystem
    if not input_file or not os.path.isfile(input_file):    
        return
    
    # Global variable initializations
    num_tweets_with_unicode = 0
    has_unicode = False

    # Extract tweet text and timestamp with generator
    for text_and_time in extract_tweet_text_and_timestamp(input_file):
        text = text_and_time[0]
        time_stamp = text_and_time[1]
        
        # Proceed only with non basic latin characters
        text = remove_non_basic_latin_chars(text)
        
        # tweet was composed of non basic latin chars
        if not text:
            continue
            
        # Clean out text
        clean_text, has_unicode = clean_string(text)
        clean_text = clean_text.strip()
          
        # Sanity Check on timestamp
        clean_time, _ = clean_string(time_stamp)
        
        if (has_unicode):
            num_tweets_with_unicode += 1
    
        # Write to output file
        try:
            with open(output_file, 'a') as f:
                if not is_empty(clean_text) and is_not_only_punc(clean_text):
                    f.write("{} (timestamp: {})\n".format(clean_text, clean_time))
        except IOError:
            sys.stderr.write("[process_tweets] - Error: Could not open {}".format(output_file))
            sys.exit(-1)
    
    # Write number of tweet with unicode to file
    try:
        with open(output_file, 'a') as f:
            f.write("{} tweets contained unicode.\n".format(num_tweets_with_unicode))
    except IOError:
        sys.stderr.write("[process_tweets] - Error: Could not open {}".format(output_file))
        sys.exit(-1)

        
class InsightChallengeSolution(object):
    
    def __init__(self, input_filename, output_filename):
        ''' Initiate Solution Object '''
        CONST = _Const()
        self.update_interval = CONST.HASH_GRAPH_UPDATE_INTERVAL
        self.time_graph_last_modified = ''
        self.input_file = input_filename
        self.output_file = output_filename
        self.num_tweets_with_unicode = 0
        self.time_of_latest_tweet = 0
        self.text = ''
        self.timestamp = 0
        self.lastupdate = 0
        self.set_of_tags = set()
        
        # Tables to hold Tracking and Routing information of hashtags.
        self.tweet_time_hashtag_graph = OrderedDict() # Track time and associating hashtags
        self.hashtag_graph = graph.Graph() # Final graph of hashtag to hashtag
        
        
        
    
    '''
        Solution of feature 2 - Hash Tag Graph
    '''
    
    def build_hashtag_graph(self):
        
        # First ever tweet with 2 or more distinct hashtag timestamp
        t1 = '' 
        
        # Extract tweet text and timestamp with generator
        for text_and_time in extract_tweet_text_and_timestamp(self.input_file):
            self.text = text_and_time[0]
            self.timestamp = text_and_time[1]

            # Proceed only with non basic latin characters
            self.text = remove_non_basic_latin_chars(self.text)

            # tweet was composed of non basic latin chars
            if not self.text:
                continue

            # Clean out text
            self.text, has_unicode = clean_string(self.text)
            self.text = self.text.strip()

            # Sanity Check on timestamp
            self.timestamp, _ = clean_string(self.timestamp)
        
            # Retrieve hashtags if any in tweet
            if (r'#' in self.text):
                self.set_of_tags = self.get_hashtags()
            else:
                continue # No hashtags

            # No need to proceed if the hashtags no valid has tag was retrieved
            # Update hashtag graph, creating edges for 2 or more distinct tags 
            if (len(self.set_of_tags) > 1):
                
                # Track time of creating the hashtags
                self.tweet_time_hashtag_graph[self.timestamp] = self.set_of_tags
                all_times = self.tweet_time_hashtag_graph.keys()
                
                if not t1: # Assign only once.
                    t1 = all_times[0]
                    self.time_graph_last_modified = t1 
                t2 = self.timestamp
                
                # Create all permutations of edges
                self.update_graph(list(self.set_of_tags), False)
                self.update_graph(list(self.set_of_tags), True)
                
                # Check if time window has elaspsed.
                # remove edges of first hashtags in tweet_time_hashtag_graph map
                if (self.hashtag_time_window_elapsed((t1, t2))):
                    self.time_graph_last_modified = t2
                    self.remove_hashtags_edge(self.tweet_time_hashtag_graph[t1])
                    
                
                # Calculate Average Degree and Write to file
                avg_deg = self.hashtag_graph_average_degrees()
                try:
                    with open(self.output_file, 'a') as f:
                        f.write("{}\n".format(avg_deg))
                except IOError:
                    sys.stderr.write("[build_hashtag_graph] - Error: Could not open {}".format(output_file))
                    sys.exit(-1)
                        
    
    '''
        Helper function: Update graph from a list of vertices tha define a sub-graph.
        
        Queue the vertices and create a edge from a vertex to the next in line using
        queue rotatations.
        Rotate queue clockwise, so 1st --> last, 2nd --> 1st, 3rd --> 2nd ...
        
        If hashtags are #Apache, #Hadoop, #Storm, 
        we must create the following permutations(order matters) of edges:
        
            1. #Apache -- #Hadoop
            2. #Hadoop -- #Storm
            3. #Storm -- #Apache
            4. #Storm -- #Hadoop
            5. #Hadoop -- #Apache
            6. #Apache -- #Storm
        
        
        :type List[str]: list_of_vertices 
        :type reverse: boolean - reverse list to create second set of edge combinations
        :type direction to rotate list (-n ==> n steps to left, n ==> n steps to right) : int
    '''
    def update_graph(self, list_of_vertices, reverse, dir=-1):
        if (len(list_of_vertices) < 2):
            return
        
        # For second set of edge permuations from list
        if (reverse):
            list_of_vertices.reverse()
            
        q = deque(list_of_vertices)
        
        # Add vertices and edges.
        for _ in xrange(len(list_of_vertices)):
            self.hashtag_graph.add_vertex(q[0]) # Add vertex into graph
            self.hashtag_graph.add_edge((q[0], q[1])) # Create edge between both vertex
            q.rotate(dir)
        # Clean up
        q.clear()
        
    
    '''
        Calculate Graph Average Degrees
    '''
    def hashtag_graph_average_degrees(self):
        # Rounding to two decimal places
        return round(self.hashtag_graph.get_graph_average_degrees(), 2) 
    
    
    '''
        Find path between two hashtags in graph to enable path deletion
    '''
    def find_hashtag_path(self, st, en):
        return self.hashtag_graph.find_path(st, en)
    
    '''
        Remove edges between hashtags
    '''
    def remove_hashtags_edge(self, list_of_vertices, dir=-1):
        if (len(list_of_vertices) < 2):
            return
            
        q = deque(list_of_vertices)
        
        for _ in xrange(len(list_of_vertices)):
            self.hashtag_graph.remove_edge((q[0], q[1]))
            q.rotate(dir)
        
        # Clean up
        q.clear()
    
    
    '''
        Extract hashtags from text if any
        Account for unicode and possibility of mulitple #s
    '''
    
    def get_hashtags(self):
        #Fetch and clean hashtags from tweet
        temp_text = self.text.lower()
        return set([re.sub(r"#+", "#", k) for k in set([re.sub(r"(\W+)$", "", j, flags = re.UNICODE) for j in set([i for i in temp_text.split() if i.startswith("#")])])])

    
    
    '''
        Function that returns the time of a tweet
        
        :type str: timestamp
        :rtype List[str]
    '''
    def get_tweet_time(self):
        return self.timestamp.strip().split()[3].split(':')
    
    
    '''
        Function that returns date time format from tweet format
        
        :type str: Tweet time string
        :rytpe: datetime object
    '''
    def format_tweettime_to_datetime(self, time_str):
        time_arr = time_str.strip().split()
        temp = time_arr[1:3]
        temp.append(time_arr[-1])
        temp.append(time_arr[3])
        return ' '.join(temp)
   

    '''
        Function that calculates if time window is elaspsed
        
        :type tuple(timestamp strings): Tuple of two tweet timestamp
        :rtype: boolean
    '''
    def hashtag_time_window_elapsed(self, timestamps):
        (t1, t2) = tuple(timestamps)
        
        # Times didn't have only digits
        if (not t1 or not t2):
            return
        
        t1 = self.format_tweettime_to_datetime(t1)
        t2 = self.format_tweettime_to_datetime(t2)
        
        dt1 = datetime.strptime(t1, '%b %d %Y %H:%M:%S')
        dt2 = datetime.strptime(t2, '%b %d %Y %H:%M:%S')
        if ((dt1 - dt2).seconds >= self.update_interval):
            return True
        return False
    
    
    '''
        Function that shows our current graph state
    '''
    def display_hashtag_graph(self):
        print self.hashtag_graph
    
    
    '''
        Function converts string array to int array
        
        Asssumes all elements are string representations of valid non-negative numbers.
    '''
    def to_int(list_of_strs):
        l = len(list_of_strs)
        nt =  [int(i) for i in list_of_strs if i.isdigit()]
        return nt if (l == len(nt)) else []
    
            
    ################# SCRIPT EXECUTION #######################
if __name__ == '__main__':
    
    file_dir = os.path.dirname(os.path.realpath('__file__'))
    input_file = file_dir + '/tweet_input/tweets.txt'
    output_file = file_dir + '/tweet_output/ft1.txt'
    output_file2 = file_dir + '/tweet_output/ft2.txt'
    
    # Get input and output file
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: python {} path_to_input_file path_to_output_file\n".format((sys.argv[0])))
        sys.exit(-1)

    if not os.path.exists(sys.argv[1]):
        sys.stderr.write("ERROR: Input file {} was not found!\n".format((sys.argv[1])))
        sys.exit(-1)
    
    ############ FEATURE 1 ##################
    print_out("Starting Feature 1")
    
    # Simulate spining cursor
    spinning_cursor()
    
    process_tweets(input_file, output_file)
    print_out("Done with Feature 1")
    
    ############ FEATURE 2 ##################
    
    # Solution to feature 2
    print_out("Starting Feature 2")
    
    # Simulate spining cursor
    spinning_cursor()
    
    solution_2 = InsightChallengeSolution(input_file, output_file2)
    solution_2.build_hashtag_graph()
    print_out("Done with Feature 2")
    
    
    # For Debugging do not uncomment unless needed.
    #solution_2.display_hashtag_graph()
    
    print_out("Done. OK!")
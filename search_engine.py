from bs4 import BeautifulSoup
import mysql.connector
from math import log10
from collections import defaultdict
from __builtin__ import True, float
import re 
import json
import time 


def build_table():
    '''build tables in database'''
    mycursor.execute("CREATE TABLE doc_url( docID VARCHAR(15), url VARCHAR(10000))")
    mycursor.execute("CREATE TABLE a_f( word VARCHAR(500), docID VARCHAR(15), docFQ INT, tf_idf DECIMAL(4,4) )")
    mycursor.execute("CREATE TABLE g_k( word VARCHAR(500), docID VARCHAR(15), docFQ INT, tf_idf DECIMAL(4,4) )")
    mycursor.execute("CREATE TABLE l_p( word VARCHAR(500), docID VARCHAR(15), docFQ INT, tf_idf DECIMAL(4,4) )")
    mycursor.execute("CREATE TABLE q_u( word VARCHAR(500), docID VARCHAR(15), docFQ INT, tf_idf DECIMAL(4,4) )")
    mycursor.execute("CREATE TABLE v_z( word VARCHAR(500), docID VARCHAR(15), docFQ INT, tf_idf DECIMAL(4,4) )")
   
   
def add_to_db(): 
    '''add webpage text into database'''
    with open("bookkeeping.tsv") as file:
        for line in file:
            global total_urls
            total_urls += 1 
            extract_data(line) #takes directory 
         
         
def re_parser(str):
    word = re.compile("[a-zA-Z]{3,}")
    return re.findall(word, str)
            
def extract_data(line):
    '''parse text with BeautifulSoup library and add word frequency'''
    directory = line.split()
    strbuild = "webpages/WEBPAGES_RAW/" + directory[0]
    with open(strbuild) as text:
        parsed_text = BeautifulSoup(text, 'html.parser')
        encoded_text = parsed_text.get_text().encode("ASCII")
        word_bank = defaultdict(int)
        new_text = parse_line(encoded_text)
        for token in new_text:
            low = token.lower()
            global_dict[low] += 1
            word_bank[low] += 1
    
        if (len(word_bank) != 0):
            parse_doc_url(directory[0],directory[1])
            global added_urls
            added_urls += 1
            for key,value in word_bank.items():
                put_into_letter_table(find_table_name(key), directory[0], key, value)
         
def parse_line(line):
    '''strips line of non-alphabetic characters and converts all letters to lowercase'''
    new_line = ''
    for i in line:
        #print(ord(i))        
        if (ord(i) >= 65 and ord(i) <= 90):
            new_line += chr(ord(i) + 32)

        elif (ord(i) >= 97 and ord(i) <= 122):
            new_line += i
        else:
            new_line += ' '
    
    return new_line      
        
def query_finder(key): 
    '''find links related to search and write into file'''
    # key is the word looked up 
    #can use for query look up 
    low_key = key.lower()
    textbuild = low_key + ".txt"
    search_file = open(textbuild,"w") 
        
    table = find_table_name(low_key)
    #select count(*) from g_k where word = 'irvine';
    #select * from g_k where word = 'irvine' order by docFQ desc limit 20;
    mycursor.execute("select Search.docID, Search.docFQ, doc_url.url \
    from (select * from {} where word = '{}' order by docFQ desc limit 20) as Search \
    inner join doc_url on Search.docID = doc_url.docID \
    order by Search.docFQ desc".format(table,low_key))
    myresult = mycursor.fetchall()
    for val in myresult:
        search_file.write(val[2].encode("ASCII") + "\n")
    #this will get top 20 docID 
    
    mycursor.execute("SELECT COUNT(*) FROM {} WHERE word = \'{}\'".format(table,low_key))
    total_find = mycursor.fetchall()
    for find in total_find:
        search_file.write("Total links found for search word: {}".format(find[0]))
    
    search_file.close()

def find_table_name(key):
    '''return the name of correct table'''
    #print key
    first_letter = ord(key[0])
    #print first_letter
    if (first_letter >= 97) and (first_letter <= 102):
        return "a_f"
    elif (first_letter >= 103) and (first_letter <= 107):
        return "g_k"
    elif (first_letter >= 108) and (first_letter <= 112):
        return "l_p"
    elif (first_letter >= 113) and (first_letter <= 117):
        return "q_u"
    elif (first_letter >= 118) and (first_letter <= 122):
        return "v_z"
    else:
        return "-1"

def put_into_letter_table(table_name,doc_id, key, value):
    '''insert word, document ID and document frequency'''
    if (table_name == "-1"):
        print "There is an error"
    else:
        mycursor.execute("INSERT INTO {} (word, docID, docFQ) VALUES (\'{}\', \'{}\', {})".format(table_name, key, doc_id, value))
        mydb.commit()

def parse_doc_url(first, second):
    '''insert the document ID and its url'''
    sql = "INSERT INTO doc_url (docID, url) VALUES (%s, %s)"
    val = (first, second)
    mycursor.execute(sql, val)
    mydb.commit()
   
def build_index():
    '''build index for database'''
    mycursor.execute("create index index_af on a_f (word)") 
    mydb.commit()
    
    mycursor.execute("create index index_gk on g_k (word)") 
    mydb.commit()
    
    mycursor.execute("create index index_lp on l_p (word)") 
    mydb.commit()
    
    mycursor.execute("create index index_qu on q_u (word)") 
    mydb.commit()
    
    mycursor.execute("create index index_vz on v_z (word)") 
    mydb.commit()
    
def add_tf_idf():
    '''add tf_idf ranking for word'''
    table_list = ['a_f', 'g_k', 'l_p', 'q_u', 'v_z']
    for table_name in table_list:
        mycursor.execute("SELECT * FROM {}".format(table_name))
        myresult = mycursor.fetchall()
        #print "Done fetching"
        for word, docID, docFQ, _ in myresult:
            
            #print 1+ log10(docFQ), docFQ
            tf_idf = round( (1+log10(docFQ)) * get_idf(word,table_name),4)
            #update test_table set tf_idf = 1.654 where col_1 = 1 and col_2 = 'first';
            mycursor.execute("UPDATE {} set tf_idf = {} where word = \'{}\' and docID = \'{}\'".format(table_name, tf_idf, word, docID))
            mydb.commit()

def get_idf(word,table):
    '''select word frequency'''
    if table == '-1':
        return -1
    mycursor.execute("SELECT count(*) FROM {} where word = \'{}\' ".format(table,word))
    myresult = mycursor.fetchall()
    
    if myresult[0][0] == 0:
        return -1
       
    else:
        #print log10(36642.0/myresult[0][0]), myresult[0][0]
        return log10(36642.0/myresult[0][0])  

def launch_query_search():
    '''user friendly interface with instructions'''
    
    print ("Ben's Search Query (minimum length for a word is 3)")
    print ("Type !quit end search query")
    user_in = raw_input("Enter your query here: ")
   
    while(user_in != '!quit'):
        query_parse(user_in)
                  
        user_in = raw_input("Enter your query here: ")
        
    print ("Exited Search Query")
    
    
def one_term(word):
    '''search algorithm for one word queries'''
    idf_1_word = get_idf(word,find_table_name(word))
    if idf_1_word == -1:
        print "There are no results for this word:", word
        return
    dict_tfidf = build_dict(word, idf_1_word)
    sorted_dict = sorted(dict_tfidf.items(), key=lambda kv: -kv[1])
    counter = 1
    for k,v in sorted_dict:
        if counter < 21:
            print "{}.".format(counter),book_keep_dict[k]
            counter+=1

def two_term(new_list):
    '''search algorithm for two word queries based on tf-idf to find best link relating to both words'''

    idf_1 = get_idf(new_list[0],find_table_name(new_list[0]))
    idf_2 = get_idf(new_list[1],find_table_name(new_list[1]))

    if idf_1 == -1 and idf_2 != -1:
        print "There are no results for this word:", new_list[0]
        one_term(new_list[1])
        
    elif idf_2 == -1 and idf_1 != -1:
        print "There are no results for this word:", new_list[1]
        one_term(new_list[0])
    
    elif idf_1 != -1 and idf_2 != -1:
        word_1_dict = build_dict(new_list[0],idf_1)
        word_2_dict = build_dict(new_list[1],idf_2)
        compare_dict(word_1_dict, word_2_dict)
        
    else:
        print "There was no match for the terms."
    
def all_terms(mylist):
    '''search algorithm for multiple word queries'''

    docID_tfidf = defaultdict(float)
    docID_occ = defaultdict(int)
    
    for term in mylist:
        idf = get_idf(term,find_table_name(term))
        if idf != -1:
            word_dict = build_dict(term, idf)
            for k,v in word_dict.items():
                #print k, v
                docID_tfidf[k] += v
                docID_occ[k] += 1
    #sorted_dict = sorted(dict_tfidf.items(), key=lambda kv: -kv[1])
    
    occ_list = sorted(docID_occ.items(), key = lambda kv: -kv[1])
    #print set(docID_occ.values())
    if len(occ_list) == 0:
        print "There were no matches for any of the terms."
    else:
        counter = 1
        #print occ_list 
        occ_num = occ_list[0][1]
        #print occ_num 
        temp = defaultdict(float)
        for k,v in occ_list:
            #print occ_num
            if v == occ_num:
                temp[k] = docID_tfidf[k]
            
            else:
                sorted_temp = sorted(temp.items(), key = lambda kv: -kv[1])
                for mk, mv in sorted_temp:
                    if counter < 21:
                        print "{}.".format(counter),book_keep_dict[mk]
                        counter+=1
                temp = defaultdict(float)
                occ_num = v
                temp[k] = docID_tfidf[k]
        
        sorted_temp2 = sorted(temp.items(), key = lambda kv: -kv[1])
        for lastk, lastv in sorted_temp2:
            if counter < 21:
                print "{}.".format(counter),book_keep_dict[lastk]
                counter+=1
                
def build_dict(word,idf):
    mycursor.execute("select * from {} where word = \'{}\' ".format(find_table_name(word),word))
    myresult = mycursor.fetchall()
    word_dict = dict()
    for _, docID, docFQ, _ in myresult:
        #print 1+ log10(docFQ), docFQ, idf
        tf_idf = round( (1+log10(docFQ)) * idf ,4)
        #print word, tf_idf, docID
        word_dict[docID] = tf_idf
    
    return word_dict

def compare_dict(dict_1, dict_2):
    '''helper function for two term search, and displays results after sorting based on tf-idf ranking'''
    global book_keep_dict
    high_dict = dict()
    low_dict = dict()
    for k,v in dict_1.items():
        try:
            high_dict[k] = v + dict_2[k]
        except KeyError:
            low_dict[k] = v
            
    for k,v in dict_2.items():
        if not high_dict.has_key(k) and not low_dict.has_key(k):
            low_dict[k] = v

    sorted_high_dict = sorted(high_dict.items(), key=lambda kv: -kv[1])
    counter = 1
  
    for k,v in sorted_high_dict:
        if counter < 21:
            print "{}.".format(counter),book_keep_dict[k]
            counter+= 1
    if counter < 20:
        sorted_low_dict = sorted(low_dict.items(), key=lambda kv: -kv[1])

        for k,v in sorted_low_dict:
            if counter < 21:
                #print(data[k])
                print "{}.".format(counter),book_keep_dict[k]
                counter+= 1

def query_parse(line):
    '''parse the user input '''
    global book_keep_dict
    start = time.time()
    new_list = []
    alist = line.split()
    for word in alist:
        if word.isalpha() and len(word)>=3:
            new_list.append(word.lower())
            
    new_list = list(set(new_list))
    print "Searched Words:", " ".join([x for x in new_list] ), "\n"
    #print new_list
    list_len = len(new_list)
    if list_len == 0:
        print "Arguments are less than 3 letters"
    
    elif list_len == 1:
        one_term(new_list[0])
        
    elif list_len == 2:
        two_term(new_list)
  
    else:
        all_terms(new_list)
        #multiple queries 
           
    end = time.time()
    print "\nSearch time for this for query search:", end-start, "\n"

if __name__ == "__main__":
    #read_file()
    #design choice to add only words greater than 3 
    start = time.time()
    mydb = mysql.connector.connect(
                host="",
                user="",
                passwd="",
                database=""
            )
    mycursor = mydb.cursor()
    print "Time to connect to DB:", time.time() - start
    
    global_dict = defaultdict(int) 
    total_urls = 0
    added_urls = 0
    
    s1 = time.time()
    with open('bookkeeping.json') as json_file:  
        book_keep_dict = json.load(json_file) # this is a dictionary 
    print "Time to load Book Keeping Dict:", time.time()- s1, "\n"
    
#     s2 = time.time()
#     build_index() # 33 seconds 
#     print time.time() - s2

    launch_query_search()

    
    
    
    
    

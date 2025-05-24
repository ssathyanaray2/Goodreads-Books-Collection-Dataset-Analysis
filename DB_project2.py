#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pymongo import MongoClient
from pprint import pprint
import re
import matplotlib.pyplot as plt
import seaborn as sns


# In[2]:


client = MongoClient("localhost",27017 )


# In[3]:


db = client.Goodreads


# In[4]:


book_reviews_updated = db["updated_book_reviews"]


# In[ ]:





# # Preprocessing

# In[5]:


# Extract publication year and decade


# In[6]:


def pre_process_publication_date():

    regx = re.compile(".{4}$")

    book_reviews_updated.update_many(

        {
            "publication_date": {
            "$type": 2
            }
        },

        [{
            "$set": { 
                "decade": { 
                    "$concat": [ {"$substr": [ "$publication_date", { "$add": [ { "$strLenCP": "$publication_date" }, -4 ] }, 3 ] }, "0"],
                },
                "publication_year":{
                    "$substr": [ "$publication_date", { "$add": [ { "$strLenCP": "$publication_date" }, -4 ] }, 4 ]
                }
            },
        }]
    );


# In[7]:


pre_process_publication_date()


# In[ ]:





# In[8]:


#Extracting the Genres


# In[9]:


def pre_process_genre():

    book_reviews_updated.update_many(
        { 
            "genres": {
                "$type": 2
            }
        }, 
        [{
            "$set": { 
                "genres_array": { 
                    "$trim" : {
                        "input" : "$genres",
                        "chars": "[]"
                    }
                },
            },
        }]
    );

    book_reviews_updated.update_many(
        { 
            "genres": {
                "$type": 2
            }
        }, 
        [{
            "$set": { 
                "genres_array": { 
                    "$replaceAll" : {
                        "input" : "$genres_array",
                        "find" : "'",
                        "replacement": ""
                    }
                },
            },
        }]
    );

    book_reviews_updated.update_many({}, 
        [{
            "$set": { 
                "genres_array": { 
                    "$split": ["$genres_array", ", "] 
                }
            },

        }]
    );


# In[10]:


pre_process_genre()


# In[ ]:





# In[11]:


#Extracting top 5 genres


# In[12]:


def get_genre_list(top, year=None):

    if year!=None :

        genre_list = book_reviews_updated.aggregate([
            { 
                "$match":{ "decade": str(year) }

            },

            { "$unwind": "$genres_array" },
            {
                "$group": {"_id": "$genres_array", "count": { "$sum": 1 }}
            },

            {
                "$group": {"_id": "null", 
                    "dictionaryField": {
                        "$push": {
                          "k": "$_id",
                          "v": "$count"
                        }
                    }
                }
            },

            { "$unwind" : "$dictionaryField" },

            { "$sort": { "dictionaryField.v": -1 } },

            {
                "$replaceRoot": {
                    "newRoot": "$dictionaryField" 
                }
            },

            { "$limit" : top}
        ])

    else :
        genre_list = book_reviews_updated.aggregate([

            { "$unwind": "$genres_array" },
            {
                "$group": {"_id": "$genres_array", "count": { "$sum": 1 }}
            },

            {
                "$group": {"_id": "null", 
                    "dictionaryField": {
                        "$push": {
                          "k": "$_id",
                          "v": "$count"
                        }
                    }
                }
            },

            { "$unwind" : "$dictionaryField" },

            { "$sort": { "dictionaryField.v": -1 } },

            {
                "$replaceRoot": {
                    "newRoot": "$dictionaryField" 
                }
            },

            { "$limit" : top}
        ])

    return genre_list


# In[ ]:





# In[13]:


genre_list = []
top = 7
genre_cursor = get_genre_list(top)
for i in genre_cursor:
    genre_list.append(i['k'])


# In[14]:


genre_list


# In[ ]:





# In[ ]:





# # Query 1

# In[15]:


#extracting no. of book publication and average rating and average number of ratings


# In[16]:


def query(genre_list,decade=None):

    avg_data=None

    if decade!=None:
        avg_data = book_reviews_updated.aggregate([

            { "$unwind" : "$genres_array" },

            { "$group" : 
                { "_id": {"decade_grp" : "$decade", "genre_grp": "$genres_array"},
                "count": {"$sum": 1},
                "average_rating": {"$avg": "$rating_score" }, 
                "average_num_rating": {"$avg": "$num_ratings" }
                }
            },

            { "$unwind": "$_id" },

            {
                "$match":{
                    "$and":[
                        {"_id.genre_grp": { "$in": genre_list}},
                        {"_id.decade_grp" : str(decade)},
                        {"_id.decade_grp" : {"$exists": "true"}}
                    ]
                }
            },

            ])

    else:
        avg_data = book_reviews_updated.aggregate([

            { "$unwind" : "$genres_array" },

            { "$group" : 
                { "_id": { "decade_grp":"$decade", "genre_grp": "$genres_array" }, 
                  "count": {"$sum": 1},
                "average_rating": {"$avg": "$rating_score" }, 
                "average_num_rating": {"$avg": "$num_ratings" }
                }
            },

            { "$unwind": "$_id" },

            {
                "$match":{
                    "$and":[
                        {"_id.genre_grp": { "$in": genre_list}},
                        {"_id.decade_grp" : {"$exists": "true"}}
                    ]
                }
            },
            

            ])

    return avg_data


# In[17]:


avg_data = query(genre_list)


# In[18]:


publication_count =[]
genres = []
decade = []
average_rating = []
average_num_rating = []


# In[19]:


for data in avg_data:
    publication_count.append(data['count'])
    genres.append(data['_id']['genre_grp'])
    decade.append(data['_id']['decade_grp'])
    average_rating.append(int(data['average_rating']))
    average_num_rating.append(int(data['average_num_rating']))


# In[20]:


sorted_indices = sorted(range(len(decade)), key=lambda k: decade[k])
decade = [decade[i] for i in sorted_indices]
genres = [genres[i] for i in sorted_indices]
publication_count = [publication_count[i] for i in sorted_indices]
average_rating = [average_rating[i] for i in sorted_indices]
average_num_rating = [average_num_rating[i] for i in sorted_indices]


# In[21]:


plt.figure(figsize = (10,5))
sns.lineplot(x = decade, y= publication_count, hue =  genres)
plt.xlabel("decade")
plt.ylabel("no. of books published per genre")


# In[22]:


quality = []
for i in range(0, len(average_num_rating)):
    quality.append(average_num_rating[i]*average_rating[i])         


# In[23]:


plt.figure(figsize = (10,5))
sns.lineplot(x = decade, y= quality , hue =  genres)
plt.xlabel("decade")
plt.ylabel("Quality of genre")


# In[ ]:





# In[ ]:





# In[ ]:





# # Query 2

# In[24]:


def query2(genre_list,decade=None):

    avg_data=None

    if decade!=None:
        avg_data = book_reviews_updated.aggregate([

            { "$unwind" : "$genres_array" },

            { "$group" : 
                { "_id": {"decade_grp" : "$decade", "genre_grp": "$genres_array"},
                "count": {"$sum": 1},
                "average_current_readers": {"$avg": "$current_readers" },
                "average_want_to_read": {"$avg": "$want_to_read" }
                }
            },

            { "$unwind": "$_id" },

            {
                "$match":{
                    "$and":[
                        {"_id.genre_grp": { "$in": genre_list}},
                        {"_id.decade_grp" : str(decade)},
                        {"_id.decade_grp" : {"$exists": "true"}}
                    ]
                }
            },

            ])

    else:
        avg_data = book_reviews_updated.aggregate([

            { "$unwind" : "$genres_array" },

            { "$group" : 
                { "_id": { "decade_grp":"$decade", "genre_grp": "$genres_array" }, 
                  "count": {"$sum": 1},
                "average_current_readers": {"$avg": "$current_readers" },
                "average_want_to_read": {"$avg": "$want_to_read" }
                }
            },

            { "$unwind": "$_id" },

            {
                "$match":{
                    "$and":[
                        {"_id.genre_grp": { "$in": genre_list}},
                        {"_id.decade_grp" : {"$exists": "true"}}
                    ]
                }
            },
            

            ])

    return avg_data


# In[25]:


avg_data = query2(genre_list)


# In[26]:


decade = []
genres = []
average_want_to_read = []
average_current_readers = []


# In[27]:


for data in avg_data:
    
    genres.append(data['_id']['genre_grp'])
    decade.append(data['_id']['decade_grp'])
    average_want_to_read.append(int(data['average_want_to_read']))
    average_current_readers.append(int(data['average_current_readers']))


# In[28]:


sorted_indices = sorted(range(len(decade)), key=lambda k: decade[k])
decade = [decade[i] for i in sorted_indices]
genres = [genres[i] for i in sorted_indices]
average_want_to_read = [average_want_to_read[i] for i in sorted_indices]
average_current_readers = [average_current_readers[i] for i in sorted_indices]


# In[29]:


popularity = []
for i in range(0, len(average_num_rating)):
    popularity.append(average_want_to_read[i]+average_current_readers[i])         


# In[30]:


plt.figure(figsize = (10,5))
sns.lineplot(x = decade, y= popularity , hue =  genres)
plt.xlabel("decade")
plt.ylabel("Popularity of genre")


# In[ ]:





# In[ ]:





# # Query 3

# In[ ]:





# In[ ]:





# In[31]:


#Add title_length to the dataset


# In[123]:


def calculate_title_length():


    book_reviews_updated.update_many(

        {
            "title": {
            "$type": 2
            }
        },

        [{
            "$set": {
                "title_length": {
                    "$strLenCP": "$title"  # Counts the number of UTF-8 code points in the title
                }
            },
        }]
    );


# In[124]:


calculate_title_length()


# In[125]:


def stats(column_name):
    
    #pipeline
    stats = [
        {
            "$match":{
                        "publication_date": {
                        "$type": 2
                        }
            }
        },
        {
            "$group": {
                "_id": "$decade",
                "avg_": {"$avg": f"${column_name}"},
                "sorted_lengths": {"$push": f"${column_name}"}
            }
           
        },
        {
            "$project": {
                "_id": 0,
                "decade": "$_id",
                "avg_": 1,
                "median_": {
                    "$let": {
                        "vars": {
                            "sortedLengths": "$sorted_lengths",
                            "count": {"$size": "$sorted_lengths"}
                        },
                        "in": {
                            "$cond": [
                                {"$gte": [{"$size": "$sorted_lengths"}, 1]},  # Check if there is at least 1 element in the array
                                {
                                    "$cond": [
                                        {"$eq": [{"$mod": ["$$count", 2]}, 0]},
                                        {
                                            "$avg": {
                                                "$slice": [
                                                    "$sorted_lengths",
                                                    {"$toInt": {"$subtract": [{"$divide": ["$$count", 2]}, 1]}},
                                                    2
                                                ]
                                            }
                                        },
                                        {"$arrayElemAt": ["$sorted_lengths", {"$toInt": {"$divide": [{"$subtract": ["$$count", 1]}, 2]} }]}
                                    ]
                                },
                                None  # If there are no elements, set median_length to None
                            ]
                        }
                    }
                }
            }
        }
    ]
    result = list(book_reviews_updated.aggregate(stats))
    return result


# In[126]:


result = stats("title_length")


# In[127]:


avg_title_length = []
for r in result:
    avg_title_length.append(r['avg_'])
decade = []
for r in result:
    decade.append(r['decade'])    

median_title_length = []
for r in result:
    median_title_length.append(r['median_'])


# In[128]:


sorted_indices = sorted(range(len(decade)), key=lambda k: decade[k])
decade = [decade[i] for i in sorted_indices]
avg_title_length = [avg_title_length[i] for i in sorted_indices]
median_title_length = [median_title_length[i] for i in sorted_indices]


# In[149]:


# Plotting
plt.figure(figsize=(10, 5))

# Left Y-axis
sns.lineplot(x=decade, y=avg_title_length, color='blue')
# Right Y-axis
plt.xlabel('Decades')
plt.ylabel('avg title length', color='blue')
plt.xticks(decade, rotation = 45)
ax2 = plt.twinx()
sns.lineplot(x=decade, y=median_title_length, color='red', ax=ax2)

plt.title('avg title length and median title length')

ax2.set_ylabel('median length', color='red')



plt.show()


# In[ ]:





# In[130]:


#analyse Average current reader and want to read


# In[131]:


result = stats("current_readers")


# In[132]:


avg_readers = []
for r in result:
    avg_readers.append(r['avg_'])
median_readers = []
for r in result:
    median_readers.append(r['median_'])
decade = []
for r in result:
    decade.append(r['decade'])
sorted_indices = sorted(range(len(decade)), key=lambda k: decade[k])
decade = [decade[i] for i in sorted_indices]
avg_readers = [avg_readers[i] for i in sorted_indices]
median_readers = [median_readers[i] for i in sorted_indices]


# In[133]:


result = stats("want_to_read")


# In[134]:


w_avg_readers = []
for r in result:
    w_avg_readers.append(r['avg_'])
w_median_readers = []
for r in result:
    w_median_readers.append(r['median_'])
decade = []
for r in result:
    decade.append(r['decade'])
sorted_indices = sorted(range(len(decade)), key=lambda k: decade[k])
decade = [decade[i] for i in sorted_indices]
w_avg_readers = [w_avg_readers[i] for i in sorted_indices]
w_median_readers = [w_median_readers[i] for i in sorted_indices]


# In[135]:


popularity_avg = []
for i in range(0, len(w_avg_readers)):
    popularity_avg.append(w_avg_readers[i]+avg_readers[i])    


# In[136]:


popularity_median = []
for i in range(0, len(w_avg_readers)):
    popularity_median.append(w_median_readers[i]+median_readers[i])   


# In[137]:


# Plotting
plt.figure(figsize=(10, 5))

# Left Y-axis
sns.lineplot(x=decade, y=popularity_avg, color='blue')
# Right Y-axis
plt.xlabel('Decades')
plt.ylabel('average popularity', color='blue')
plt.xticks(decade, rotation = 45)
ax2 = plt.twinx()
sns.lineplot(x=decade, y=popularity_median, color='red', ax=ax2)

plt.title('average popularity and median popularity')

ax2.set_ylabel('median popularity', color='red')

plt.tight_layout()


plt.show()


# In[138]:


#pearson correlation metric a number between –1 and 1 that measures the strength and direction of the relationship between two variables.


# In[141]:


def find_covariance(column1_name, column2_name):
    # Calculate the mean of each column
    pipeline_means = [
        {
            "$group": {"_id": None, 
                    "mean_column1": {"$avg": f"${column1_name}"}, 
                    "mean_column2": {"$avg": f"${column2_name}"}}
        }
    ]

    mean_result = list(book_reviews_updated.aggregate(pipeline_means))
    mean_column1 = (mean_result[0]['mean_column1'])
    mean_column2 = mean_result[0]['mean_column2']



    pipeline_std_dev = [
            {"$group": {"_id": None, 
                        "std_dev_column1": {"$stdDevSamp": f"${column1_name}"}, 
                        "std_dev_column2": {"$stdDevSamp": f"${column2_name}"}}}
        ]

    std_dev_result = list(book_reviews_updated.aggregate(pipeline_std_dev))

    if std_dev_result:
        std_dev_column1 = std_dev_result[0]['std_dev_column1']
        std_dev_column2 = std_dev_result[0]['std_dev_column2']
    else:
        print("Standard deviation calculation failed.")
        std_dev_column1 = None
        std_dev_column2 = None

    # Pipeline to calculate covariance between column1 and column2

    pipeline_subtract=[
        
        {"$group": {"_id": 0, 
        "covariance" : {"$avg": {"$multiply":[{"$subtract": [f"${column1_name}", mean_column1]},{"$subtract": [f"${column2_name}", mean_column2]}]} }
            }
        }
    ]

    covariance_result = list(book_reviews_updated.aggregate(pipeline_subtract))
    covariance = covariance_result[0]['covariance']
    correlation = covariance / (std_dev_column1 * std_dev_column2)
    return correlation


# In[143]:


print("Pearson Correlation coefficient between want to read and title length : " , find_covariance("want_to_read",  "title_length"))
print("Pearson Correlation coefficient between current readers and title length : " , find_covariance( "title_length","current_readers"))


# In[ ]:





# In[ ]:





# In[120]:


with open("positive_words.txt", "r") as file:
    # Read the entire contents of the file
    content = file.readlines()


# Print the content of the file
positive_list = [word.strip() for word in content]


# In[121]:


with open("negative_words.txt", "r") as file:
    # Read the entire contents of the file
    content = file.readlines()


# Print the content of the file
negative_list = [word.strip() for word in content]


# In[102]:


def word_count_tag():
    pipeline = [
        {
            "$project": {
                "title": 1,
                "description": 1,
                "positive_word_count": {
                    "$size": {
                        "$filter": {
                            "input": {"$split": ["$description", " "]},
                            "as": "word",
                            "cond": {"$in": ["$$word", positive_list]}
                        }
                    }
                },
                "negative_word_count": {
                    "$size": {
                        "$filter": {
                            "input": {"$split": ["$description", " "]},
                            "as": "word",
                            "cond": {"$in": ["$$word", negative_list]}
                        }
                    }
                }
            }
        },
        {
            "$addFields": {
                "tag": {
                    "$cond": {
                        "if": {"$gt": ["$positive_word_count", "$negative_word_count"]},
                        "then": "positive",
                        "else": {
                            "$cond": {
                                "if": {"$lt": ["$positive_word_count", "$negative_word_count"]},
                                "then": "negative",
                                "else": "neutral"
                            }
                        }
                    }
                }
            }
        }
    ]


    # Execute the aggregation pipeline
    results = list(db.updated_reviews2.aggregate(pipeline))
    return results
    # # Print the results
    # for result in results:
    #     print(f"Book: {result['title']}, Positive Words: {result['positive_word_count']}, Negative Words: {result['negative_word_count']}")


# In[103]:


results = word_count_tag()


# In[104]:


tag= []
for result in results:
     tag.append(result['tag'])
current_readers = []
want_to_read = []
for document in db["updated_reviews2"].find({}, {"current_readers": 1, "want_to_read": 1}):
    if "want_to_read" in document and "current_readers" in document:
        current_readers.append(document["current_readers"])
        want_to_read.append(document["want_to_read"])

plt.figure(figsize = (15,10))
sns.scatterplot(x = current_readers, y = want_to_read, hue = tag)
plt.xlabel("current readers")
plt.ylabel("want to read")

plt.show()


# In[105]:


tag= []
for result in results:
     tag.append(result['tag'])
current_readers = []
want_to_read = []
for document in db["updated_reviews2"].find({}, {"current_readers": 1, "want_to_read": 1}):
    if "want_to_read" in document and "current_readers" in document:
        current_readers.append(document["current_readers"])
        want_to_read.append(document["want_to_read"])

plt.figure(figsize = (15,10))
sns.scatterplot(x = current_readers, y = want_to_read, hue = tag)
plt.xlabel("current readers")
plt.ylabel("want to read")
plt.xlim(-5, 5000)  # Adjust these limits according to your data
plt.ylim(0, 40000)
plt.show()


# In[ ]:





# In[ ]:





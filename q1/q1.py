from mrjob.job import MRJob  

class WordLength(MRJob):
    def mapper(self, key, line):
        words = line.split()  # Split the line into words
        for w in words:  # Iterate through the words
            yield (w[0], len(w)), 1  # Emit a key-value pair for the first letter of the word and its length, with a count of 1
            
    def reducer(self, key, values):  
        yield key, sum(values)  # Emit a key-value pair for the word length and its total count

if __name__ == '__main__':
    WordLength.run() 

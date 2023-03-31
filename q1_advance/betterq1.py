from mrjob.job import MRJob

class WordLength(MRJob):
    def mapper_init(self):
        self.length_counts = {}  # initialize an empty dictionary to hold the counts of words with each starting letter and length combination
        self.cache = {}  # initialize an empty cache dictionary
        self.cache_size = 1000  # set cache size limit to 1000
    
    def mapper(self, key, line):
        words = line.split()  # split the input line into words
        for w in words:
            key = (w[0], len(w))
            if key in self.cache:  # if the current key-value pair is already in the cache dictionary
                self.cache[key] += 1  # increment its value
            else:  # if the current key-value pair is not yet in the cache dictionary
                self.cache[key] = 1  # add it to the dictionary with a value of 1
            if len(self.cache) > self.cache_size:
                for k, v in self.cache.items():
                    self.length_counts[k] = self.length_counts.get(k, 0) + v  # add the cache values to the length_counts dictionary
                self.cache = {}  # empty the cache dictionary
    
    def mapper_final(self):
        for k, v in self.cache.items():
            self.length_counts[k] = self.length_counts.get(k, 0) + v  # add any remaining cache values to the length_counts dictionary
        for key, value in self.length_counts.items():  # emit each key-value pair in the length_counts dictionary
            yield key, value
    
    def reducer(self, key, values):
        yield key, sum(values)  # sum the values associated with each key and yield the key-value pair

if __name__ == '__main__':
    WordLength.run()



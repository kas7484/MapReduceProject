
from mrjob.job import MRJob
import csv


class RetailStep(MRJob):

   
    def mapper_init(self):
        # Initialize the total items sold and total amount spent to zero
        self.total_items_sold = 0
        self.total_amount_spent = 0
        # Initialize the cache dictionary to store intermediate results
        self.cache = {}

  
    def mapper(self, key, line):
        # Parse the line using the csv module
        parts = list(csv.reader([line]))[0]
        # Skip the header row
        if parts[0] == 'InvoiceNo':
            return
        # Extract the country, stock code, quantity, and unit price from the line
        country = parts[7]
        stock_code = parts[1]
        quantity = int(parts[3])
        unit_price = float(parts[5])
        # Calculate the amount spent on the item
        amount_spent = quantity * unit_price
        # Increment the total items sold and total amount spent for all items
        self.total_items_sold += quantity
        self.total_amount_spent += amount_spent
        # Add the quantity and amount spent for the item to the cache
        key = (country, stock_code)
        if key in self.cache:
            self.cache[key] = (self.cache[key][0] + quantity, self.cache[key][1] + amount_spent)
        else:
            self.cache[key] = (quantity, amount_spent)
        # Flush the cache if it exceeds a certain size
        if len(self.cache) >= 1000:
            for k, v in self.cache.items():
                yield k, v
            self.cache = {}

   
    def mapper_final(self):
        # Yield any remaining items in the cache
        for k, v in self.cache.items():
            yield k, v

    
    def reducer_init(self):
        # Initialize the total items sold and total amount spent to zero
        self.total_items_sold = 0
        self.total_amount_spent = 0
        # Initialize the cache dictionary to store intermediate results
        self.cache = {}

    
    def reducer(self, key, values):
        # Skip the ('Total', 'Total') lines
        if key != ('Total', 'Total'):
            # Add the quantities and amounts spent for the item to the cache
            if tuple(key) in self.cache:
                self.cache[key] = (self.cache[key][0] + sum(v[0] for v in values),
                                   self.cache[key][1] + sum(v[1] for v in values))
            else:
                self.cache[tuple(key)] = (sum(v[0] for v in values), sum(v[1] for v in values))            # Flush the cache if it exceeds limit
            if len(self.cache) >= 1000:
                for k, v in self.cache.items():
                    yield k, v
                self.cache = {}

    # Define the reducer_final method that is called after all key-value pairs have been processed
    def reducer_final(self):
        # Yield any remaining items in the cache
        for k, v in self.cache.items():
            yield k, v

if __name__ == '__main__':
    RetailStep.run()
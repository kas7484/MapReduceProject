from mrjob.job import MRJob 
import csv 

class RetailStep(MRJob):

    def mapper(self, key, line):
        parts = list(csv.reader([line]))[0]  # Parse the CSV line into a list
        if parts[0] == 'InvoiceNo':  # Check if the row is a header row
            return  # Skip the header row
        country = parts[7]  # Get the country code
        stock_code = parts[1]  # Get the stock code
        quantity = int(parts[3])  # Get the quantity as an integer
        unit_price = float(parts[5])  # Get the unit price as a float
        amount_spent = quantity * unit_price  # Calculate the total amount spent
        yield (country, stock_code), (quantity, amount_spent)  # Emit a key-value pair for the country, stock code, quantity, and amount spent

    def reducer(self, key, values):  
        total_quantity = 0  # Initialize a total counter for quantity
        total_amount_spent = 0.0  # Initialize a total counter for amount spent
        for quantity, amount_spent in values:  # Iterate through the values
            total_quantity += quantity  # Add the quantity to the total
            total_amount_spent += amount_spent  # Add the amount spent to the total
        if key != ["Total", "Total"]:  # Check if the key is not ["Total", "Total"]
            yield key, (total_quantity, total_amount_spent)  # Emit a key-value pair for the country, stock code, total quantity, and total amount spent


if __name__ == '__main__':
    RetailStep.run()

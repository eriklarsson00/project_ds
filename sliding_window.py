from collections import defaultdict

def sliding_window_connections(window_size, input_string):
    # split string to words
    words = input_string.split()
    
    # dictionary to store word connections
    connections = defaultdict(int)
    
    # loop through string using sliding window
    for i in range(len(words) - window_size + 1):
        # get the current window
        window = words[i:i + window_size]
        
        # iterate over all word pairs within the window
        for j in range(window_size - 1):
            # form word pairs: (word, next_word)
            word_pair = (window[j], window[j + 1])
            # increment the count for this word pair in the dictionary
            connections[word_pair] += 1
    
    return dict(connections)

# example
input_string = "this is an example of a sliding window over words. this is nothing but a test."
window_size = 3

connections_dict = sliding_window_connections(window_size, input_string)

# print results
for pair, count in connections_dict.items():
    print(f"{pair}: {count}")




def sliding_window_connections_no_overlap(window_size, input_string):
    # Split the input string into words
    words = input_string.split()
    
    # Initialize a dictionary to store word connections
    connections = defaultdict(int)
    
    # Loop through the string, stepping by window_size
    for i in range(0, len(words) - window_size + 1, window_size):
        # Get the current window
        window = words[i:i + window_size]
        
        # Iterate over all word pairs within the window
        for j in range(window_size - 1):
            # Form word pairs: (word, next_word)
            word_pair = (window[j], window[j + 1])
            # Increment the count for this word pair in the dictionary
            connections[word_pair] += 1
    
    return dict(connections)

# Example usage:
input_string = "This is an example of a sliding window over words. This is a test."
window_size = 4

# Get the dictionary of connections
connections_dict = sliding_window_connections_no_overlap(window_size, input_string)

# Print the result
for pair, count in connections_dict.items():
    print(f"{pair}: {count}")
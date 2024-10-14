from collections import defaultdict

def sliding_window_connections(window_size, input_string):
    # Split string into words
    words = input_string.split()
    
    # Dictionary to store word connections
    connections = defaultdict(int)
    
    # Loop through string using a sliding window
    for i in range(len(words) - window_size + 1):
        # Get the current window
        window = words[i:i + window_size]
        
        # Iterate over all pairs of words within the window (not just adjacent ones)
        for j in range(window_size):
            for k in range(j + 1, window_size):
                word_pair = (window[j], window[k])
                reverse_pair = (window[k], window[j])

                # Ensure that the pair is stored in the order (word, next_word)
                if reverse_pair in connections:
                    # If the reverse pair exists, add its count to the correct pair
                    connections[word_pair] += connections.pop(reverse_pair)
                else:
                    # Increment the count for the correct pair
                    connections[word_pair] += 1
    
    # Remove any pairs with zero counts (if they were mistakenly added)
    return {pair: count for pair, count in connections.items() if count > 0}




def sliding_window_connections_no_overlap(window_size, input_string):
    # Split the input string into words
    words = input_string.split()
    
    # Initialize a dictionary to store word connections
    connections = defaultdict(int)
    
    # Loop through the string, stepping by window_size
    for i in range(0, len(words) - window_size + 1, window_size):
        # Get the current window
        window = words[i:i + window_size]
        
        # Iterate over all pairs of words within the window (not just adjacent ones)
        for j in range(window_size):
            for k in range(j + 1, window_size):
                word_pair = (window[j], window[k])
                reverse_pair = (window[k], window[j])

                # Ensure that the pair is stored in the order (word, next_word)
                if reverse_pair in connections:
                    # If the reverse pair exists, add its count to the correct pair
                    connections[word_pair] += connections.pop(reverse_pair)
                else:
                    # Increment the count for the correct pair
                    connections[word_pair] += 1
    
    # Remove any pairs with zero counts (if they were mistakenly added)
    return {pair: count for pair, count in connections.items() if count > 0}

# Example usage:
input_string = "This is an example of a sliding window over words. This is a test."
window_size = 4

# Get the dictionary of connections
connections_dict = sliding_window_connections_no_overlap(window_size, input_string)

# Print the result
for pair, count in connections_dict.items():
    print(f"{pair}: {count}")

from collections import defaultdict

def sliding_window_connections(max_window_size, input_string):
    # Split the string into words
    words = input_string.split()
    
    # Create a list of dictionaries, one for each window size
    connections_list = [defaultdict(int) for _ in range(max_window_size - 1)]
    
    # Start with the smallest window size, k = 2, and build up
    for k in range(2, max_window_size + 1):
        # Loop through the words with a sliding window of size `k`
        for i in range(len(words) - k + 1):
            window = words[i:i + k]
            
            # If k == 2, add adjacent word pairs only
            if k == 2:
                word_pair = (window[0], window[1])
                reverse_pair = (window[1], window[0])
                
                # Ensure pairs are stored in one direction
                if reverse_pair in connections_list[k - 2]:
                    connections_list[k - 2][word_pair] += connections_list[k - 2].pop(reverse_pair)
                else:
                    connections_list[k - 2][word_pair] += 1
            else:
                # For larger windows, only add pairs involving the last word in the window
                last_word = window[-1]
                first_word = window[0]
                word_pair = (first_word, last_word)
                reverse_pair = (last_word, first_word)
                    
                    # Ensure pairs are stored in one direction
                if reverse_pair in connections_list[k - 2]:
                    connections_list[k - 2][word_pair] += connections_list[k - 2].pop(reverse_pair)
                else:
                    connections_list[k - 2][word_pair] += 1

    # Convert each defaultdict to a regular dict for easier readability
    result = [dict(connection) for connection in connections_list]
    return result

max_window_size = 4
test_string = "This is an exmaple of a sliding window over words. This is a test."

# Get the connections for each window size up to `max_window_size`
connections_by_window = sliding_window_connections(max_window_size, test_string)
print(f'hello, {connections_by_window}')
# Display the results
for k, connections in enumerate(connections_by_window, start=2):
    print(f"Window size {k}:")
    print(connections)
    print()


from collections import Counter

def get_all_pairs_for_window_size(k, connections_by_window):
    combined = Counter()
    for i in range(k - 1):
        combined.update(connections_by_window[i])
    return dict(combined)

# Example for window size 3
all_pairs_for_k3 = get_all_pairs_for_window_size(4, connections_by_window)
print("All pairs for window size 3:", all_pairs_for_k3)




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


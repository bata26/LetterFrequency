import threading
import sys
import time

mutex = threading.Lock()
num_letters = 0

letter_frequency = {}

def count_chunk_letters(arr, count):
    global num_letters
    local_count = {}
    
    for string in arr:
        for l in string:
            letter = l.lower()
            if letter >= 'a' and letter <= 'z':
                if letter not in local_count:
                    local_count[letter] = 1
                else:
                    local_count[letter] += 1

    with mutex:
        for letter, cnt in local_count.items():
            if letter not in count:
                count[letter] = cnt
            else:
                count[letter] += cnt
        num_letters += sum(local_count.values())

def main():
    start_time = time.time()

    global num_letters, letter_frequency

    with open(sys.argv[1], "r") as file:
        lines = file.readlines()
    
    threads = []
    n = int(sys.argv[2])
    chunk_size = len(lines) // n

    for i in range(n):
        start_index = i * chunk_size
        end_index = start_index + chunk_size if i != n - 1 else len(lines)
        thread = threading.Thread(target=count_chunk_letters, args=(lines[start_index:end_index], letter_frequency))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    et = time.time() - start_time

    with open("output.csv", "w") as file:
        file.write("Letter,Frequency\n")
        for letter, freq in letter_frequency.items():
            file.write(f"{letter},{freq/num_letters}\n")
        file.write(f"Total,{num_letters}\n")
        file.write(f"Exection time,{et}\n")

if __name__ == "__main__":
    main()

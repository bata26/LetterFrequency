import os

def repeat_content_until_size(input_file, output_file, target_size):
    try:
        # Leggi il contenuto del file di input
        with open(input_file, 'rb') as f:
            content = f.read()
    except IOError as e:
        return f"Impossibile leggere il file {input_file}: {e}"

    # Verifica se il contenuto del file di input è vuoto
    if len(content) == 0:
        return "Il file di input è vuoto"

    try:
        # Apri il file di output per l'append
        with open(output_file, 'ab') as f:
            # Calcola il numero di ripetizioni necessarie
            repeat_count = target_size // len(content)
            remainder = target_size % len(content)

            # Scrivi il contenuto ripetuto nel file di output
            for _ in range(repeat_count):
                f.write(content)
            if remainder > 0:
                f.write(content[:remainder])
    except IOError as e:
        return f"Impossibile scrivere nel file {output_file}: {e}"

    print(f"Il contenuto è stato ripetuto e salvato in {output_file} con dimensione {target_size} byte.")
    return None

def main():
    dimensions = [50, 500, 1, 3, 10]
    languages = ["IT"]
    for lang in languages:
        input_file = f"moby_{lang}.txt"
        for dim in dimensions:
            dimension_string = "MB"
            if dim <= 10:
                dimension_string = "GB"
            output_file = f"{lang}_{dim}{dimension_string}.txt"
            target_size = 1024 * 1024 * dim
            if dimension_string == "GB":
                target_size *= 1024
            err = repeat_content_until_size(input_file, output_file, target_size)
            if err:
                print("Errore:", err)

if __name__ == "__main__":
    main()
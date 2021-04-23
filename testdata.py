import json
import sys
import random

if len(sys.argv) < 3:
    print("usage <number of generated entries> <template path> <output path>")
    exit(0)


generated_entries = int(sys.argv[1])
template_file = str(sys.argv[2])
output_file = str(sys.argv[3])


def read_template_file(path):
    with open(path, 'r') as reader:
        content = reader.read()
    return json.loads(content)


#generator functions
def template_choice(t):
    choices = t['values']
    def closure():
        return random.choice(choices)

    return closure


def template_random_integer(t):
    min_int = int(t.get("min", 0))
    max_int = int(t.get("max", 100))
    step = float(t.get("step", 1))

    def closure():
        return str(random.randrange(min_int, max_int, step))

    return closure


def template_random_float(t):
    f_min = float(t.get("min", 0))
    f_max = float(t.get("max", 100))
    f_range = f_max - f_min

    def closure():
        return str(f_min + random.random() * f_range)

    return closure


def template_random_boolean(t):
    probability = t.get("probability", 0.5)
    boolean_format = t.get("format", "numeric")

    def format_result(b):
        if boolean_format == "numeric":
            return "1" if b else "0"
        elif boolean_format == "text":
            return "True" if b else "False"

    def closure():
        return format_result(True if random.random() < probability else False)
    return closure


def compile_entry_generators(template_dict):

    generator_types = {
        "choice": template_choice,
        "int": template_random_integer,
        "float": template_random_float,
        "boolean": template_random_boolean
    }

    generators = {}
    for attr in template_dict.keys():
        template = template_dict[attr]
        generators[attr] = generator_types[template["type"]](template)
    return generators


def generate_entries(template_dict, generators, no_entries):
    # add first row for column identification
    entries = [list(template_dict.keys())]
    for i in range(no_entries):
        entry = []
        for attr in template_dict.keys():
            entry.append(generators[attr]())
        entries.append(entry)
    return entries


def create_csv(entries):
    rows = []
    for entry in entries:
        row = ";".join(entry)
        rows.append(row)
    return "\n".join(rows)


def write_output_file(path, content):
    with open(path, 'w') as writer:
        writer.write(content)

template_dictionary = read_template_file(template_file)
#create dict of entry generators from template file
entry_generators = compile_entry_generators(template_dictionary)
#generate entries
entries = generate_entries(template_dictionary, entry_generators, generated_entries)
#write entries to csv
write_output_file(output_file, create_csv(entries))




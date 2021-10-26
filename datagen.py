import json
import sys
import math
import time
import random
import threading
import traceback
import concurrent.futures
import multiprocessing
from queue import Queue

if len(sys.argv) < 3:
    print("usage <dataset size> <template path> <output path>")
    exit(0)


dataset_size = int(sys.argv[1])
template_file = str(sys.argv[2])
output_file = str(sys.argv[3])


max_workload = 10000
workers = max(1, multiprocessing.cpu_count())
if dataset_size < 1000:
    workers = 1


def read_template_file(path):
    with open(path, 'r') as reader:
        content = reader.read()
    return json.loads(content)


# utility functions
def clamp(value, min_value, max_value):
    return min(max_value, max(min_value, value))


# generator functions
def template_spread(t):
    generator = build_generator(t["generator"])
    column_count = t["columns"]

    def closure():
        columns = []
        for i in range(column_count):
            columns.append(generator())
        return columns
    return closure

def template_join(t):
    generators = []
    separator = t.get("separator", " ")
    for g in t["generators"]:
        generators.append(build_generator(g))

    def closure():
        results = []
        for gen in generators:
            results.append(gen())
        return separator.join(results)
    return closure


def template_choice(t):
    choices = t['values']
    weights = t.get('weights', None)

    if weights is not None and len(choices) == len(weights):
        weights = map(int, weights)
        cumulative_weights = []
        weight = 0
        for w in weights:
            weight += w
            cumulative_weights.append(weight)

        def closure():
            return random.choices(choices, cum_weights=cumulative_weights)[0]

        return closure
    else:
        def closure():
            return random.choice(choices)
        return closure



expression_globals = {
    # Math
    "math": math,
    "floor": math.floor,
    "sqrt": math.sqrt,
    # Random
    "random": random.random,
    "gauss": random.gauss,
    # Custom
    "clamp": clamp}


def template_expression(t):
    expression = compile(t.get("expr", "0"), "<string>", "eval")

    def closure():
        return str(eval(expression, expression_globals))

    return closure


def template_random_integer(t):
    min_int = int(t.get("min", 0))
    max_int = int(t.get("max", 100))
    step = int(t.get("step", 1))

    def closure():
        return str(random.randrange(min_int, max_int, step))

    return closure


def template_random_float(t):
    f_min = float(t.get("min", 0))
    f_max = float(t.get("max", 100))
    f_range = f_max - f_min
    f_format = t.get('format', '{:.4f}')

    def closure():
        return f_format.format(f_min + random.random() * f_range)

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


generator_factories = {
    "const": template_const,
    "choice": template_choice,
    "int": template_random_integer,
    "float": template_random_float,
    "boolean": template_random_boolean,
    "spread": template_spread,
    "join": template_join,
    "expression": template_expression,
}


def build_generator(template):
    return generator_factories[template["type"]](template)


def compile_entry_generators(template):
    generators = {}
    for column_name in template.keys():
        column_template = template[column_name]
        generators[column_name] = build_generator(column_template)
    return generators


# Creates tuple with (column_name,generator_key)
def list_columns(template):
    columns = []
    for column_name in template.keys():
        column_template = template[column_name]
        if column_template["type"] == "spread":
            number_of_columns = column_template["columns"]
            digits = 1 + math.floor(math.log10(number_of_columns))
            for index in range(number_of_columns):
                identifier = ("{:0>"+str(digits)+"}").format(index)
                columns.append((f"{column_name}-{identifier}", str(column_name)))
        else:
            column = (column_name, column_name)
            columns.append(column)
    return columns


def generate_entries(pipeline, event, column_templates, generators, entry_count):
    pipeline.workers += 1

    try:
        for i in range(entry_count):
            entry = []
            for attr in column_templates.keys():
                if column_templates[attr]["type"] == "spread":
                    entry = entry + generators[attr]()
                else:
                    entry.append(generators[attr]())
            pipeline.put_entry(entry)
        pipeline.workers -= 1
    except Exception as e:
        traceback.print_exc()
        sys.exit(-1)


def csv_format(entry):
    try:
        return ";".join(entry)
    except TypeError:
        print(f"Could not format {entry}")


def write_output_file(pipeline, event, path):
    with open(path, 'w') as writer:
        while not event.is_set() or not pipeline.empty():
            writer.write(csv_format(pipeline.get_entry()) + '\n')


class Pipeline(Queue):
    workers = 0
    submitted = 0
    buffered = 0
    complete = 0
    header = False

    _tstart = 0
    _clast = 0
    _tlast = 0

    def predict(self):
        now = time.time()
        if self._tlast == 0:
            self._tstart = now
            self._tlast = now
            return 0
        deltat = now - self._tstart
        deltac = self.complete

        remaining = dataset_size - self.complete

        if deltat <= 0:
            return 0
        eps = deltac / deltat
        self.c_last = self.complete
        self._tlast = now

        return remaining / eps

    def put_entry(self, entry):
        self.submitted += 1
        self.buffered += 1
        self.put(entry, True)

    def put_header(self, header):
        self.header = True
        self.put(header, True)

    def get_entry(self):
        value = self.get(True)
        if self.header:
            self.header = False
            return value
        self.buffered -= 1
        self.complete += 1
        if self.complete % 10 == 0 and self.complete == dataset_size:
            print(f'\r{self.workers}\t{self.complete}\t{self.buffered}\t{dataset_size}\t'+'{:4.2f}'.format((self.complete / dataset_size) * 100) + '\t' + time.strftime('%H:%M:%S', time.gmtime(self.predict())), end='\r')
        return value



column_templates = read_template_file(template_file)
column_names = list_columns(column_templates)
#create dict of column generators from template file
column_data_generators = compile_entry_generators(column_templates)
#generate entries
done_event = threading.Event()
pipeline_queue = Pipeline(maxsize=1000)
threads = []
remaining_entries = dataset_size
workloads = []
while remaining_entries > 0:
    workload = min(remaining_entries, dataset_size // workers, max_workload)
    remaining_entries -= workload
    workloads.append(workload)



write_thread = threading.Thread(target=write_output_file,args=(pipeline_queue, done_event, output_file))
write_thread.start()

if include_header:
    pipeline_queue.put_header([column[0] for column in column_names])

try:
    print(f'Workers\tWritten\tBuffer\tTotal\t%\tETA')
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        threads = [executor.submit(generate_entries, *(pipeline_queue, done_event, column_templates, column_data_generators, thread_workload)) for thread_workload in workloads]
    done_event.set()
except Exception as ex:
    print(ex)
    done_event.set()
    for thread in threads:
        thread.cancel()
    write_thread.join()
print("\r\n", end="")





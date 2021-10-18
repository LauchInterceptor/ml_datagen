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


def compile_entry_generators(template_dict):
    generators = {}
    for attr in template_dict.keys():
        template = template_dict[attr]
        generators[attr] = build_generator(template)
    return generators


completed_entries = 0


def generate_entries(pipeline, event, template_dict, generators, entry_count):
    pipeline.workers += 1
    # print('Started worker with {number} entries'.format(number=entry_count))
    # add first row for column identification
    try:
        for i in range(entry_count):
            entry = []
            for attr in template_dict.keys():
                if template_dict[attr]["type"] == "spread":
                    entry = entry + generators[attr]()
                else:
                    entry.append(generators[attr]())
            pipeline.put_entry(entry)
        pipeline.workers -= 1
    except Exception as e:
        traceback.print_exc()
        sys.exit(-1)


def csv_format(entry):
    return ";".join(entry)


def write_output_file(pipeline, event, path):
    with open(path, 'w') as writer:
        while not event.is_set() or not pipeline.empty():
            writer.write(csv_format(pipeline.get_entry()) + '\n')


class Pipeline(Queue):
    workers = 0
    submitted = 0
    buffered = 0
    complete = 0

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

    def get_entry(self):
        value = self.get(True)
        self.buffered -= 1
        self.complete += 1
        if self.complete % 10 == 0 or self.complete == dataset_size:
            print(f'\r{self.workers}\t{self.complete}\t{self.buffered}\t{dataset_size}\t'+'{:4.2f}'.format((self.complete / dataset_size) * 100) + '\t' + time.strftime('%H:%M:%S', time.gmtime(self.predict())), end='\r')
        return value



template_dictionary = read_template_file(template_file)
#create dict of entry generators from template file
entry_generators = compile_entry_generators(template_dictionary)
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

print(f'Workers\tWritten\tBuffer\tTotal\t%\tETA')
with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
    threads = [executor.submit(generate_entries, *(pipeline_queue, done_event, template_dictionary, entry_generators, thread_workload)) for thread_workload in workloads]
done_event.set()




import csv
from collections import defaultdict
from db_manage import ContainerDatabase
import re


def is_valid_train(train_name):
    return re.match(r'^[A-Za-zА-Яа-я0-9]+$', train_name) is not None

def parse_container(container):
    match = re.match(r'^K\d{3}([A-Z]{2})(\d+)$', container)
    if not match:
        raise ValueError(f"Invalid container format: {container}")
    ctype = match.group(1)
    weight = int(match.group(2))
    return ctype, weight

def distribute_containers(m, b, db_path='data.db', output_file='output.csv'):
    db = ContainerDatabase(db_path)
    data = db.get_data()
    
    container_options = defaultdict(list) # {container: [(train, priority),]}
    container_info = {}  # {container: (type, weight)}
    f = 0
    for container, train, priority in data:
        if priority < b:
            f = 1
            ctype, weight = parse_container(container)
            container_options[container].append((train, priority))
            container_info[container] = (ctype, weight)
                
    if not f:
        raise ValueError(f"-2")
    
    # Если для контейнера нет поездов с приоритетом < b
    for container in container_options:
        if not container_options[container]:
            raise ValueError(f"-2")

    # Инициализация счетчиков
    trains = {
        'counts': defaultdict(int),
        'weights': defaultdict(int),
        'types': defaultdict(lambda: defaultdict(int)),
        'priorities': defaultdict(int)
    }

    # Сортировка контейнеров: сначала тяжелые, потом с меньшим выбором
    sorted_containers = sorted(
        container_options.keys(),
        key=lambda x: (-container_info[x][1], len(container_options[x]))
    )

    allocation = {}

    for container in sorted_containers:
        ctype, weight = container_info[container]
        options = container_options[container]
        
        best_train = None
        max_score = float('-inf')
        
        # Рассчитываем средние значения
        total_containers = len(allocation)
        avg_count = total_containers / len(trains['counts']) if trains['counts'] else 0
        avg_weight = sum(trains['weights'].values()) / len(trains['weights']) if trains['weights'] else 0

        for train, priority in options:
            if not is_valid_train(train):
                continue
            # Расчет показателей
            type_score = trains['types'][train][ctype] * 500
            count_penalty = -abs((trains['counts'][train] + 1) - avg_count) * 200
            priority_score = -priority * 10
            weight_penalty = -abs((trains['weights'][train] + weight) - avg_weight) * 0.5
            score = type_score + count_penalty + priority_score + weight_penalty 

            if score > max_score:
                max_score = score
                best_train = train

        # Назначаем контейнер выбранному поезду, изменяем счетчики
        allocation[container] = best_train
        trains['counts'][best_train] += 1
        trains['weights'][best_train] += weight
        trains['types'][best_train][ctype] += 1
        trains['priorities'][best_train] += next(p for t, p in options if t == best_train)

    # Проверка ограничения на разницу в количестве
    #if trains['counts']:
    #    counts = list(trains['counts'].values())
    #    print(trains['counts'])
    #    if max(counts) - min(counts) > m:
    #        raise ValueError("-1")
            
    # Сохранение результатов
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['container', 'train'])
        for container, train in allocation.items():
            writer.writerow([container, train])

    return allocation

m = 0
b = 1
#allocation = distribute_containers(m, b)

# O(K * (M log M + T))
# K - количество контейнеров

# M - среднее количество доступных поездов на контейнер

# T - среднее количество допустимых поездов на контейнер (с приоритетом < b)
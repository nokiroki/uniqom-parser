import multiprocessing
import os
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import numpy as np


DESC_PATTERN = '[^\t\n]+'

SITE = 'https://uniqom.ru/catalog/product/'
IMAGE_SITE = 'https://static.uniqom.ru/uploads/items/'


def get_data(name):
    return pd.read_excel(name)


def get_image_desc(queue, art):
    print('Started to exclude info from article ' + art)
    data = requests.get(SITE + art).text
    soup = BeautifulSoup(data, 'lxml')
    img_block = soup.find('div', class_='uk-background-contain uk-height-1-1')
    species_block = soup.find('table', class_='feip-product-attributes')

    if img_block is None and species_block is None:
        print(art + ' is totally missing!')
        queue.put((art, None))

    if img_block is None:
        image = ''
    else:
        image = requests.get('https:' + img_block['style'].split('"')[1]).content

    if species_block is None:
        species = 'EMPTY'
    else:
        species = species_block.find('td', class_='feip-product-attributes-data').text
        t = re.findall(DESC_PATTERN, species)
        species = 'EMPTY' if len(t) == 0 else re.split(r' {3,}', t[0])[1]

    print('Finish downloading art ' + art)
    queue.put((art, species, image))


def queue_reader(queue, final_results):
    print('Queue started')
    while True:
        try:
            msg = queue.get()
            if msg == 'DONE':
                print('queue closed')
                break
            if msg is not None:
                if msg[1] is None:
                    final_results.append({'article':msg[0], 'species': 'EMPTY', 'status': 'MISSING'})
                else:
                    if msg[2] != '':
                        with open('Фото/' + msg[0] + '.jpg', 'wb') as f:
                            f.write(msg[2])
                    final_results.append({'article':msg[0], 'species': msg[1], 'status': 'DONE'})
                    print(msg[0] + ' successfully added ')
        except Exception as e:
            print(e.args[0])
    return final_results


def main_cycle():
    m = multiprocessing.Manager()
    queue = m.Queue()
    data = get_data('data.XLS')
    art_list = data['Код']
    if os.path.exists('table.csv'):
        exist_art_list = pd.read_csv('table.csv')['article']
        art_list = np.setdiff1d(art_list, exist_art_list)
    r_art_list = [(queue, str(a)) for a in art_list]
    final_results = []
    pool = multiprocessing.Pool(8)
    queue_task = pool.apply_async(queue_reader, (queue, final_results))
    print(len(art_list))

    try:
        pool.starmap(get_image_desc, r_art_list[:5])
    except Exception as e:
        print(e.args[0])
    finally:
        print('Done!')
        queue.put('DONE')
        final_results = queue_task.get()
        pool.close()

    if os.path.exists('table.csv'):
        default = pd.read_csv('table.csv')
        pd.concat([default, pd.DataFrame(final_results)], ignore_index=True, axis=0) \
            .to_csv('table.csv', index=False)
    else:
        pd.DataFrame(final_results).to_csv('table.csv', index=False)


if __name__ == '__main__':
    main_cycle()







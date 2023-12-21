from slugify import slugify
import json
import csv


filepath = "foodandbevergaesdetails.json"

def read_json(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)
    return data


def sluggify_url(filepath):
    df = read_json(filepath)
    names= []

    for detail in df:
            comp ={}
            comp_name = detail['company_name']
            comp['comp_name'] = comp_name
            ID = detail['ID']
            comp['ID'] = ID
            slug = slugify(comp_name)
            comp['slug_name'] = slug
            comp['company_url'] = "https://6sense.com/company/" + slug + "/" + ID
            names.append(comp)

    return names

names = sluggify_url(filepath)


with open('slugurl.csv', 'w', encoding='utf-8', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, names[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(names)


# txt = "This is a test ---"
# r = slugify(txt)
# print(r)


# # txt = "0-60 Energy Cafe"
# # r = slugify(txt)
# # print(r)

# text = "A ! Bodytech Participacoes"


# text2 = "A & B CONTRACTORS (DEVON) LIMITED"
# text3 = "A & B Schlüsseldienst Münster"
# text4 = "A & B Precision Metals, Inc"
# r = slugify(text4)
# print(r)
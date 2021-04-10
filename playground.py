import requests
import json
import swapi
url = "http://swapi.dev/api"
r = requests.get(url=url)
# print(r.json())


f = open("starwars.json")
data = json.load(f)
f.close()

for k in data.keys():
    if k == "results":
        ships = k

        for i in data[ships]:
            for key in i.keys():
                if key == "name":
                    print(i[key])

                    
for name in ship_names:
    poster_content = name
    quantity = int(input("Enter quantity: "))
    price = float(input("Enter price: "))
    email = input("Enter email: ")
    sales_rep = input("Enter sales representative: ")
    promo_code = secure_rand()
    db.insert_into_db(posters,poster_content, quantity, price,
                      email, sales_rep, promo_code)

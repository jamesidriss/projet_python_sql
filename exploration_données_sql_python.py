# Utiliser Python et Pandas avec SQLite

import sqlite3
import pandas as pd
import psycopg2
 
connexion = sqlite3.connect('factbook.db')

# Calcul de somme 

# On peut afficher dans un tableau Pandas les sommes des surfaces terrestres et surfaces de l'eau pour chaque pays
a = pd.read_sql_query('SELECT SUM(area_land), SUM(area_water) FROM facts WHERE area_land != "";', con = connexion)

# On affiche le ratio Terre / Mer
print(a['SUM(area_land)']/a['SUM(area_water)'])

# Calcul de stats

birth_rate_tuple = connexion.execute('SELECT MAX(birth_rate) FROM facts;').fetchall()
birth_rate_count = birth_rate_tuple[0][0]
print(birth_rate_count)

population_growth_tuple = connexion.execute('SELECT MIN(population_growth) FROM facts;').fetchall()
population_growth_min = population_growth_tuple[0][0]
print(population_growth_min)
death_rate_tuple = connexion.execute('SELECT MAX(death_rate) FROM facts;').fetchall()
death_rate_max = death_rate_tuple[0][0]
print(death_rate_max)

terre_tuple = connexion.execute('SELECT SUM(area_land) FROM facts;').fetchall()
terre_som = terre_tuple[0][0]
print(terre_som)
water_tuple = connexion.execute('SELECT AVG(area_water) FROM facts;').fetchall()
water_moy = water_tuple[0][0]
print(water_moy)

# Combiner plusieurs fonctions

facts_stats = connexion.execute('SELECT AVG(population), SUM(population), MAX(birth_rate) FROM facts;').fetchall()
print(facts_stats) # Tuple avec 3 données

mean_pop = facts_stats[0][0] # On va récupérer les valeurs des tuples 1 par 1
sum_pop = facts_stats[0][1]
max_birth_rate = facts_stats[0][2]
print(mean_pop)
print(sum_pop)
print(max_birth_rate)

# Utiliser where

pop_growth_tuple = connexion.execute('SELECT AVG(population_growth) FROM facts WHERE population > 10000000;').fetchall()
pop_growth_moy = pop_growth_tuple[0][0]
print(pop_growth_moy)
	
# Sélectionner des données uniques

unique_birth_rate = connexion.execute('SELECT DISTINCT birth_rate FROM facts;').fetchall()
print(unique_birth_rate)

# Associer count et distinct

uniquemoy_birth_rate = connexion.execute('SELECT AVG(DISTINCT birth_rate) FROM facts WHERE population > 20000000;').fetchall()
uniquemoy_birth_rate = uniquemoy_birth_rate[0][0]
print(uniquemoy_birth_rate)

uniquesom_pop = connexion.execute('SELECT SUM(DISTINCT population) FROM facts WHERE area_land > 1000000;').fetchall()
uniquesom_pop = uniquesom_pop[0][0]
print(uniquesom_pop)

# Opération mathématique simple

pop_growth = connexion.execute('SELECT population_growth / 1000000.0 FROM facts;').fetchall()
print(pop_growth)

# Opération mathématique entre colonnes

pop_next_year = connexion.execute('SELECT name, population * (population_growth + 1) FROM facts;').fetchall()
print(pop_next_year)

# Exploration de données

pop = connexion.execute('SELECT AVG(population) FROM facts;').fetchall()
pop_growth = connexion.execute('SELECT AVG(population_growth) FROM facts;').fetchall()
avg_br = connexion.execute('SELECT AVG(birth_rate) FROM facts;').fetchall()
avg_dr = connexion.execute('SELECT AVG(death_rate) FROM facts;').fetchall()
print(pop)
print(pop_growth)
print(avg_br)
print(avg_dr)

# OU

averages = 'SELECT AVG(population), AVG(population_growth), AVG(birth_rate), AVG(death_rate) FROM facts;'
avg_results = connexion.execute(averages).fetchall()
pop_avg = avg_results[0][0]
pop_growth_avg = avg_results[0][1]
birth_rate_avg = avg_results[0][2]
death_rate_avg = avg_results [0][3]
print(pop_avg)
print(pop_growth_avg)
print(birth_rate_avg)
print(death_rate_avg)

# Trouver les valeurs extrêmes

minimums = 'SELECT MIN(population) AS pop_min, MIN(population_growth) AS pop_growth_min, MIN(birth_rate) AS birth_rate_min, MIN(death_rate) AS death_rate_min FROM facts;'
maximums = 'SELECT MAX(population) AS pop_max, MAX(population_growth) AS pop_growth_max, MAX(birth_rate) AS birth_rate_max, MAX(death_rate) AS death_rate_max FROM facts;'
min_results = connexion.execute(minimums).fetchall()
max_results = connexion.execute(maximums).fetchall()
pop_min = min_results[0][0]
pop_growth_min = min_results[0][1]
birth_rate_min = min_results[0][2]
death_rate_min = min_results[0][3]
pop_max = max_results[0][0]
pop_growth_max = max_results[0][1]
birth_rate_max = max_results[0][2]
death_rate_max = max_results[0][3]
print(pop_min)
print(pop_growth_min)
print(birth_rate_min)
print(death_rate_min)
print(pop_max)
print(pop_growth_max)
print(birth_rate_max)
print(death_rate_max)

# Trouver des valeurs

min_and_max = 'SELECT MIN(population) AS pop_min, MIN(population_growth) AS pop_growth_min, MIN(birth_rate) AS birth_rate_min, MIN(death_rate) AS death_rate_min, MAX(population) AS pop_max, MAX(population_growth) AS pop_growth_max, MAX(birth_rate) AS birth_rate_max, MAX(death_rate) AS death_rate_max FROM facts WHERE population > 0 AND population < 2000000000;'
results = connexion.execute(min_and_max).fetchall()
print(results)

# Prédiction croissance démographique

projected_population_query = 'SELECT ROUND(population + population * (population_growth/100), 0) FROM facts WHERE population < 7000000000 AND population > 0  AND population IS NOT NULL AND population_growth IS NOT NULL;'
projected_population = connexion.execute(projected_population_query).fetchall()
print(projected_population[0:10])

# Exploration croissance démographique

proj_pop_query = 'SELECT MIN(ROUND(population + population * (population_growth/100), 0)), MAX(ROUND(population + population * (population_growth/100), 0)), AVG(ROUND(population + population * (population_growth/100), 0)) FROM facts WHERE population < 7000000000 AND population > 0 AND population IS NOT NULL AND population_growth IS NOT NULL;'
proj_results = connexion.execute(proj_pop_query).fetchall()
prop_proj_min = proj_results[0][0]
prop_proj_max = proj_results[0][1]
prop_proj_avg = proj_results[0][2]
print(prop_proj_min)
print(prop_proj_max)
print(prop_proj_avg)

# PostgreSQL

connexion = psycopg2.connect(dbname = 'bank_accounts', user = 'batman', password = 'motdepasse13')
curseur = connexion.cursor()
curseur.execute("INSERT INTO notes VALUES (1, 'Ceci est ma première note', 'Titre note')")
curseur.execute('SELECT * FROM notes;')
rows = curseur.fetchall()
print(rows)
connexion.commit()
connexion.close()
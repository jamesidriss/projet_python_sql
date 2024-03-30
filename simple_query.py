import sqlite3

connexion = sqlite3.connect('jobs.db')
cursor = connexion.cursor()
query = 'SELECT Major FROM recent_grads;'
cursor.execute(query)  # Exécuter la requête
majors = cursor.fetchall()  # Récupérer les résultats
# Ou : majors = cursor.execute(query).fetchall()
print(majors[0:5])

# On se connecte à la base de données avec la méthode connect. On a utilisé la librairie SQLite3. 
# On applique la méthode cursor à cette variable connexion qu’on a créée.
# On écrit notre code SQL et on l’assigne à une variable Query. On l’a met entre guillemet car c’est une chaine de caractère.
# On applique la méthode execute sur le curseur avec en paramètre la requête SQL (Query ici).
# On applique la méthode fetchall sur notre curseur pour l’appliquer sur la variable locale qui comprend tous les tuples. Ça va donc chercher tous les résultats.
# On affiche les résultats avec la fonction print.
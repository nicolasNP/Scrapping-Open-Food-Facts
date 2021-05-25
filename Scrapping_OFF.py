# -*- coding: utf-8 -*-
"""
@author: Nicolas Nenny-Pirotte
"""

# Import des packages nécessaires
import requests
from bs4 import BeautifulSoup
import re
import time
import datetime
import _thread
import multiprocessing
from inspect import signature
import pandas as pd

def elapsed_time(a, b):
    '''
    Fonction de calcul du temps
    
    Paramètres :
    a : Heure à l'instant T exprimée en secondes
    b : Heure à l'instant T exprimée en secondes
    
    Retourne : 
    res : Temps de traitement mis en forme obtenu par soustraction de b par a
    '''
    elapsed = round(b - a) # Calcul du temps en seconde entre les deux moments entrés
    res = datetime.timedelta(seconds = elapsed) # Mise en forme du résultat 
    return res

def get_number_of_pages():
    '''
    Fonction de récupération du nombre de pages sur la page d'accueil d'open food facts
    
    Retourne : 
    number_of_pages : Le nombre de pages sur le site
    '''
    start = time.time() # Enregistrement de l'heure de début
    
    r = requests.get('https://fr.openfoodfacts.org/') # Définition du site cible
    soup = BeautifulSoup(r.text, 'html.parser') # Parsing du site à l'aide de beautiful soup
     # Recherche de la balise <a> ayant l'attribut rel = next$follow qui correspond au bouton "Suivante" de la liste de pages puis on remonte deux fois afin d'obtenir la dernière page indiquée
    number_of_pages = int(soup.find('a', attrs = {'rel' : 'next$nofollow'}).previous.previous)
    
    end = time.time() # Enregistrement de l'heure de fin
    result = elapsed_time(start, end) # Appel de la fonction elapsed_time afin d'obtenir le temps de traitement
    print('Récupération du nombre de page terminée en : {}'.format(result))
    return number_of_pages

def link_extract(first_page = 1, last_page = False):
    '''
    Fonction de récupération de la liste de tous les produits
    Prend uniquement 0 ou 2 paramètres en entrée
    
    Paramètres : 
    first_page : Numéro de la page à partir de laquelle on souhaite récupérer les données (Défaut : 1)
    last_page : Numéro de la page jusqu'à laquelle souhaite récupérer les données (Défaut : Dernière page)

    Retourne :
    products_list : Liste des liens des produits
    '''
    start = time.time()
    global products
    products = []
    
    params = signature(link_extract).parameters  # Vérification du nombre de paramètres entrés
    if len(params) not in [0,2]:
        print('Erreur, veuillez n\'entrer aucun paramètres ou deux')
        return
    else:
        pass

    if last_page == False:
        last_page = get_number_of_pages() # Si aucune valeur n'est donnée pour la dernière page, la fonction get_number_of_pages est appelée
    
    for page_number in range(first_page, last_page + 1, 1): # On boucle sur toutes les pages du site
        r = requests.get('https://fr.openfoodfacts.org/{}'.format(page_number))
        soup = BeautifulSoup(r.text, 'html.parser')
        iteration = 100 # Nombre de produits par page
        if page_number == last_page: # Si on attend la dernière page on compte le nombre de produits au cas où il n'y en ai pas 100
            iteration = len(soup.find_all('a', attrs = {'href' : re.compile('/produit/')}))
        
        for i in range(0, iteration): # On boucle sur ce nombre produits et on récupère le morceau de lien correspondant au produit
            products.append(soup.find_all('a', attrs = {'href' : re.compile('/produit/')})[i]['href'])
         
        # Calcul d'une estimation de temps total de traitement basé sur le temps de récupération de 30 pages
        if page_number == 30: 
            end = time.time()
            time_t = round(end - start)
            estimate = time_t * last_page / 30
            estimated = datetime.timedelta(seconds =estimate)
            result = elapsed_time(start, end)
            print('{} pages récupérées en : {}, estimation totale : {}'.format(page_number, result, estimated))
            
    end = time.time()
    result = elapsed_time(start, end)
    print('Récupération des liens terminée en : {}'.format(result))
    return products

def scrapping(link_list, dest):
    '''
    Fonction de récupération des informations voulues sur le site
    
    Paramètres : 
    link_list : Liste des liens partiels issu de la fonction link_extract
    dest : Liste où seront enregistré les informations récupérées

    Retourne :
    dest : Liste de tous les produits
    '''
    start = time.time()
    try: # Utilisation d'un try except afin de savoir si une erreur survient et sur quel lien
        for product in (link_list): # On boucle sur la liste des liens
            # La liste de lien ne contenant que la partie permettant d'identifier le produit, on la concatene avec l'adresse racine du site
            r = requests.get('https://fr.openfoodfacts.org{}'.format(product)) 
            soup = BeautifulSoup(r.text, 'html.parser')
            
            # Utilisation systèmatique de try except afin de d'assigner la valeur XXX en cas de valeur manquante
            try:
                name = soup.find('h1', attrs = {'itemprop' : 'name'}).text # Recherche de l'attribut h1 avec itemprop name et récupération du texte
            except:
                name = 'XXX'

            try:
                barcode = soup.find('span', attrs = {'id' : 'barcode'}).text # Idem que précédemment mais avec un span et son id cette fois
            except:
                barcode = 'XXX'

            try:
                nutriscore = soup.select("img[src*=nutriscore]")[0]['alt'][-1] # Recherche d'une image et de la valeur de son alt cette fois
            except:
                nutriscore = 'XXX'
            try:
                nova = soup.select("img[src*=nova-group]")[0]['alt']
            except:
                nova = 'XXX'

            try:
                ecoscore = soup.select("img[src*=ecoscore]")[1]['alt'][-1]
            except:
                ecoscore = 'XXX'

            try:
                quantity = soup.find(text = re.compile('Quantité')).next # Recherche à partir du texte et non de la balise cette fois puis sélection de la balise suivante
            except:
                quantity = 'XXX'
            try:
                condition = soup.find_all('a', attrs = {'href' : re.compile('/conditionnement/')}) # Certains champs contiennent plusieurs valeurs, on vérifie donc si c'est le cas
                if len(condition) > 1: # Si il y a plusieurs valeurs on boucle dessus et on ajoute le tout à une liste
                    condition_list = []
                    for i in range(0, len(condition)):
                        condition_list.append(condition[i].text)
                else:
                    condition_list = condition[0].text  # Sinon on récupère la première valeur trouvée
            except:
                condition_list = 'XXX'

            try:
                brand = soup.find_all('a', attrs = {'href' : re.compile('/marque/')})
                if len(brand) > 1:
                    brand_list = []
                    for i in range(0, len(brand)):
                        brand_list.append(brand[i].text)
                else:
                    brand_list = brand[0].text
            except:
                brand_list = 'XXX'

            try:
                category = soup.find_all('a', attrs = {'href' : re.compile('/categorie/')})
                if len(category) > 1:
                    category_list = []
                    for i in range(0, len(category)):
                        category_list.append(category[i].text)
                else:
                    category_list = category[0].text
            except:
                category_list = 'XXX'

            try:
                label = soup.find_all('a', attrs = {'href' : re.compile(r'/label/')})
                if len(label) > 1:
                    label_list = []
                    for i in range(0, len(label)):
                        label_list.append(label[i].text)
                else:
                    label_list = label[0].text
            except:
                label_list = 'XXX'

            try:
                ingredient_origin = soup.find_all('a', attrs = {'href' : re.compile(r'/origine/')})
                if len(ingredient_origin) > 1:
                    ingredient_origin_list = []
                    for i in range(0, len(ingredient_origin)):
                        ingredient_origin_list.append(ingredient_origin[i].text)
                else:
                    ingredient_origin_list = ingredient_origin[0].text
            except:
                ingredient_origin_list = 'XXX'

            try:
                transformation = soup.find_all('a', attrs = {'href' : re.compile(r'/lieu-de-fabrication/')})
                if len(transformation) > 1:
                    transformation_list = []
                    for i in range(0, len(transformation)):
                        transformation_list.append(transformation[i].text)
                else:
                    transformation_list = transformation[0].text
            except:
                transformation_list = 'XXX'

            try:
                trace_code = soup.find_all('a', attrs = {'href' : re.compile(r'/code-emballeur/')})
                if len(trace_code) > 1:
                    trace_code_list = []
                    for i in range(0, len(trace_code)):
                        trace_code_list.append(trace_code[i].text)
                else:
                    trace_code_list = trace_code[0].text
            except:
                trace_code_list = 'XXX'
            try:
                web_link = soup.find(text = re.compile('Lien vers la page du produit sur le site officiel du fabricant')).next.next['href']
            except:
                web_link = 'XXX'
            try:
                shops = soup.find_all('a', attrs = {'href' : re.compile('/magasin/')})
                if len(shops) > 1:
                    shops_list = []
                    for i in range(0, len(shops)):
                        shops_list.append(shops[i].text)
                else:
                    shops_list = trace_code[0].text
            except:
                shops_list = 'XXX'
            try:
                countries_sell = soup.find_all('a', attrs = {'href' : re.compile('/pays/')})
                if len(countries_sell) > 1:
                    countries_sell_list = []
                    for i in range(0, len(shops)):
                        countries_sell_list.append(countries_sell[i].text)
                else:
                    countries_sell_list = trace_code[0].text
            except:
                countries_sell_list = 'XXX'
            try:
                additives = soup.find_all('a', attrs = {'href' : re.compile(r'/additif/')})
                if len(additives) > 1:
                    additives_list = []
                    for i in range(0, len(additives)):
                        additives_list.append(additives[i].text)
                else:
                    additives_list = additives[0].text
            except:
                additives_list = 'XXX'

            try:
                palm_oil = soup.find_all('a', attrs = {'href' : re.compile(r'/ingredients-issus-de-l-huile-de-palme/')})
                if len(palm_oil) > 1:
                    palm_oil_list = []
                    for i in range(0, len(palm_oil)):
                        palm_oil_list.append(palm_oil[i].text)
                else:
                    palm_oil_list = palm_oil[0].text
            except:
                palm_oil_list = 'XXX'

            try:
                matiere_grasse = soup.find('b', text = re.compile('Matières grasses')).previous#.strip()
            except:
                matiere_grasse = 'XXX'
            try:
                acide_gras = soup.find('b', text = re.compile('Acides gras')).previous#.strip()
            except:
                acide_gras = 'XXX'
            try:
                sucres = soup.find('b', text = re.compile('Sucres')).previous#.strip()
            except:
                sucres = 'XXX'
            try:
                sel = soup.find('b', text = re.compile('Sel')).previous#.strip()
            except:
                sel = 'XXX'

            try:
                energy = soup.find('td', text = re.compile('kcal'), attrs = {'class' : 'nutriment_value'})
                
            except:
                energy = 'XXX'

            # On enregistre toutes les valeurs récupérées sous forme de tuple dans une liste
            dest.append((name,barcode,nutriscore,nova,ecoscore,quantity,condition_list,brand_list,category_list,label_list,ingredient_origin_list,transformation_list,trace_code_list,web_link,shops_list,countries_sell_list,additives_list,palm_oil_list,matiere_grasse,acide_gras,sucres,sel,energy))
            
            # Calcul d'une estimation de temps total de traitement basé sur le temps de récupération de 100 produits
            if len(dest) == 100:
                end = time.time()
                time_t = round(end - start)
                estimate = time_t * len(products) / 100
                estimated = datetime.timedelta(seconds =estimate)
                result = elapsed_time(start, end)
                print('{} produits récupérés sur {} en : {}, estimation totale : {}'.format(len(dest), len(link_list), result, estimated))
        
        end = time.time()
        result = elapsed_time(start, end)
        print('Récupération des données terminée en : {}'.format(result))
    except Exception as e: # En cas d'erreur, on affiche :
        print('https://fr.openfoodfacts.org{}'.format(product)) # Le lien où est survenue l'erreur
        print('Error: '+ str(e)) # La nature de l'erreur



def processed_link_extract(start = 1,end = False):
    '''
    Fonction de récupération de la liste de tous les produits avec multiprocessing
    Prend uniquement 0 ou 2 paramètres en entrée
    
    Paramètres : 
    start : Numéro de la page à partir de laquelle on souhaite récupérer les données (Défaut : 1)
    end : Numéro de la page jusqu'à laquelle souhaite récupérer les données (Défaut : Dernière page)

    Retourne :
    products_list : Liste des liens des produits
    '''
    params = signature(processed_link_extract).parameters  # Vérification du nombre de paramètres entrés
    if len(params) not in [0,2]:
        print('Erreur, veuillez n\'entrer aucun paramètres ou deux')
        return
    else:
        pass

    if end == False:
        end = int(get_number_of_pages())# Si aucune valeur n'est donnée pour la dernière page, la fonction get_number_of_pages est appelée]
    
    max_threads = multiprocessing.cpu_count() # Vérification du nombre de coeurs disponibles
    
    # Selon le nombre de coeurs, définition du nombre de pages assignées à chaque thread sur une base de 2 threads par coeur
    bin_value = round(int(end) / max_threads)
    bin_value2 = bin_value + 1
    bin_value3 = (bin_value * 2) + 1
    bin_value4 = (bin_value * 3) + 1
    bin_value5 = (bin_value * 4) + 1
    bin_value6 = (bin_value * 5) + 1
    bin_value7 = (bin_value * 6) + 1
    bin_value8 = (bin_value * 7) + 1
    
     # Lancement du nombre de threads selon le nombre de coeurs du processeur en utilisant le découpage précédemment effectué
    if max_threads == 1:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Pages {} à {}'.format(start,bin_value))
        try:
            _thread.start_new_thread(link_extract, (start,bin_value))
        except Exception as e:
            print('Error: '+ str(e))
        
    elif max_threads == 2:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Pages {} à {}'.format(start,bin_value))
        print('Thread 2: Pages {} à {}'.format(bin_value2,end))
        try:
            _thread.start_new_thread(link_extract, (start,bin_value))
            _thread.start_new_thread(link_extract, (bin_value2,end))
        except Exception as e:
            print('Error: '+ str(e))

    elif max_threads == 4:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Pages {} à {}'.format(start,bin_value))
        print('Thread 2: Pages {} à {}'.format(bin_value2,bin_value*2))
        print('Thread 3: Pages {} à {}'.format(bin_value3,bin_value*3))
        print('Thread 4: Pages {} à {}'.format(bin_value4,end))
        try:
            _thread.start_new_thread(link_extract, (start,bin_value))
            _thread.start_new_thread(link_extract, (bin_value2,bin_value * 2))
            _thread.start_new_thread(link_extract, (bin_value3,bin_value * 3))
            _thread.start_new_thread(link_extract, (bin_value4,end))
        except Exception as e:
            print('Error: '+ str(e))

    elif max_threads == 6:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Pages {} à {}'.format(start,bin_value))
        print('Thread 2: Pages {} à {}'.format(bin_value2,bin_value*2))
        print('Thread 3: Pages {} à {}'.format(bin_value3,bin_value*3))
        print('Thread 4: Pages {} à {}'.format(bin_value4,bin_value*4))
        print('Thread 5: Pages {} à {}'.format(bin_value5,bin_value*5))
        print('Thread 6: Pages {} à {}'.format(bin_value6,end))
        try:
            _thread.start_new_thread(link_extract, (start,bin_value))
            _thread.start_new_thread(link_extract, (bin_value2,bin_value * 2))
            _thread.start_new_thread(link_extract, (bin_value3,bin_value * 3))
            _thread.start_new_thread(link_extract, (bin_value4,bin_value * 4))
            _thread.start_new_thread(link_extract, (bin_value5,bin_value * 5))
            _thread.start_new_thread(link_extract, (bin_value6,bin_value * 6))
        except Exception as e:
            print('Error: '+ str(e))

    elif max_threads == 8:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Pages {} à {}'.format(start,bin_value))
        print('Thread 2: Pages {} à {}'.format(bin_value2,bin_value*2))
        print('Thread 3: Pages {} à {}'.format(bin_value3,bin_value*3))
        print('Thread 4: Pages {} à {}'.format(bin_value4,bin_value*4))
        print('Thread 5: Pages {} à {}'.format(bin_value5,bin_value*5))
        print('Thread 6: Pages {} à {}'.format(bin_value6,bin_value*6))
        print('Thread 7: Pages {} à {}'.format(bin_value7,bin_value*7))
        print('Thread 8: Pages {} à {}'.format(bin_value8,end))
        try:
            _thread.start_new_thread(link_extract, (start,bin_value))
            _thread.start_new_thread(link_extract, (bin_value2,bin_value * 2))
            _thread.start_new_thread(link_extract, (bin_value3,bin_value * 3))
            _thread.start_new_thread(link_extract, (bin_value4,bin_value * 4))
            _thread.start_new_thread(link_extract, (bin_value5,bin_value * 5))
            _thread.start_new_thread(link_extract, (bin_value6,bin_value * 6))
            _thread.start_new_thread(link_extract, (bin_value7,bin_value * 7))
            _thread.start_new_thread(link_extract, (bin_value8,end))
        except Exception as e:
            print('Error: '+ str(e))

    else:
        print('Un nombre de coeurs inattendu a été détecté')
        return

def processed_scrapping(source, dest):
    '''
    Fonction de récupération des informations voulues sur le site avec multiprocessing
    
    Paramètres : 
    source : Liste des liens partiels issu de la fonction link_extract

    Retourne :
    dest : Liste de tous les produits
    '''
    max_threads = multiprocessing.cpu_count()

    bin_value = round(int(len(source)) / max_threads)
    bin_value2 = bin_value + 1
    bin_value3 = (bin_value * 2) + 1
    bin_value4 = (bin_value * 3) + 1
    bin_value5 = (bin_value * 4) + 1
    bin_value6 = (bin_value * 5) + 1
    bin_value7 = (bin_value * 6) + 1
    bin_value8 = (bin_value * 7) + 1
    if max_threads == 1:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Produits {} à {}'.format('1',len(source)))
        try:
            _thread.start_new_thread(scrapping, (source,dest))
        except Exception as e:
            print('Error: '+ str(e))
        
    elif max_threads == 2:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Produits {} à {}'.format('1',bin_value))
        print('Thread 2: Produits {} à {}'.format(bin_value2,len(source)))
        try:
            _thread.start_new_thread(scrapping, (source[:bin_value],dest))
            _thread.start_new_thread(scrapping, (source[bin_value2:],dest))
        except Exception as e:
            print('Error: '+ str(e))

    elif max_threads == 4:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Produits {} à {}'.format('1',bin_value))
        print('Thread 2: Produits {} à {}'.format(bin_value2,bin_value*2))
        print('Thread 3: Produits {} à {}'.format(bin_value3,bin_value*3))
        print('Thread 4: Produits {} à {}'.format(bin_value4,len(source)))
        try:
            _thread.start_new_thread(scrapping, (source[:bin_value],dest))
            _thread.start_new_thread(scrapping, (source[bin_value2:bin_value*2],dest))
            _thread.start_new_thread(scrapping, (source[bin_value3:bin_value*3],dest))
            _thread.start_new_thread(scrapping, (source[bin_value4:],dest))
        except Exception as e:
            print('Error: '+ str(e))

    elif max_threads == 6:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Produits {} à {}'.format('1',bin_value))
        print('Thread 2: Produits {} à {}'.format(bin_value2,bin_value*2))
        print('Thread 3: Produits {} à {}'.format(bin_value3,bin_value*3))
        print('Thread 4: Produits {} à {}'.format(bin_value4,bin_value*4))
        print('Thread 5: Produits {} à {}'.format(bin_value5,bin_value*5))
        print('Thread 6: Produits {} à {}'.format(bin_value6,len(source)))
        try:
            _thread.start_new_thread(scrapping, (source[:bin_value],dest))
            _thread.start_new_thread(scrapping, (source[bin_value2:bin_value*2],dest))
            _thread.start_new_thread(scrapping, (source[bin_value3:bin_value*3],dest))
            _thread.start_new_thread(scrapping, (source[bin_value4:bin_value*4],dest))
            _thread.start_new_thread(scrapping, (source[bin_value5:bin_value*5],dest))
            _thread.start_new_thread(scrapping, (source[bin_value6:],dest))
        except Exception as e:
            print('Error: '+ str(e))

    elif max_threads == 8:
        print('Nombre de threads : {}'.format(max_threads))
        print('Thread 1: Produits {} à {}'.format('1',bin_value))
        print('Thread 2: Produits {} à {}'.format(bin_value2,bin_value*2))
        print('Thread 3: Produits {} à {}'.format(bin_value3,bin_value*3))
        print('Thread 4: Produits {} à {}'.format(bin_value4,bin_value*4))
        print('Thread 5: Produits {} à {}'.format(bin_value5,bin_value*5))
        print('Thread 6: Produits {} à {}'.format(bin_value6,bin_value*6))
        print('Thread 7: Produits {} à {}'.format(bin_value7,bin_value*7))
        print('Thread 8: Produits {} à {}'.format(bin_value8,len(source)))
        try:
            _thread.start_new_thread(scrapping, (source[:bin_value],dest))
            _thread.start_new_thread(scrapping, (source[bin_value2:bin_value*2],dest))
            _thread.start_new_thread(scrapping, (source[bin_value3:bin_value*3],dest))
            _thread.start_new_thread(scrapping, (source[bin_value4:bin_value*4],dest))
            _thread.start_new_thread(scrapping, (source[bin_value5:bin_value*5],dest))
            _thread.start_new_thread(scrapping, (source[bin_value6:bin_value*6],dest))
            _thread.start_new_thread(scrapping, (source[bin_value7:bin_value*7],dest))
            _thread.start_new_thread(scrapping, (source[bin_value8:],dest))
        except Exception as e:
            print('Error: '+ str(e))
    else:
        print('Un nombre de coeurs inattendu a été détecté')
        return

def off_to_csv(data):
    off_df = pd.DataFrame(data, columns=['Nom','Code_Barre','Nutriscore','Novascore','Ecoscore','Quantite','Conditionnement','Marques','Categories','Labels','Origine_Ingredients','Pays_Transformation','Code_tracage','URL','Magasins','Pays_Vente','Additifs','Huile_Palme','Matiere_Grasse','Acide_Gras','Sucres','Sel','Energie'])
    off_df.to_csv('off.csv', index = False, encoding = 'utf-8')
    return off_df
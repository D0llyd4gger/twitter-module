#! usr/bin/python
#-*-coding: utf8 -*-

import re, codecs, base64, requests, urllib, sys, time
import simplejson as json
from types import NoneType

def credentials(infile):
	"""
	prend un nom de fichier en entree contenant la ckey et le csecret (un par ligne)
	renvoie les ckey et csecret
	"""
	with codecs.open(infile, "r", "utf-8") as f:
		ckey, csecret = f.readlines()
	return ckey.strip(), csecret.strip()

def authentication(infile):
	"""
	prend en argument un nom de fichier contenant les customer key et secret (string)
	fait une requête d'authentification
	renvoie l'access token
	si la requete échoue imprime un message
	"""
	ckey, csecret= credentials(infile)
	#encode les arguments
	code = base64.b64encode(ckey+":"+csecret)
	#arguments du header
	headers = {"Authorization": "Basic "+code,"Content-Type": "application/x-www-form-urlencoded"}
	#requete
	r = requests.post("https://api.twitter.com/oauth2/token", data = {"grant_type":"client_credentials"}, headers = headers)
	#recuperation de la reponse
	if "bearer" in r.text:
		rr = re.split("[^A-Za-z0-9%]",r.text)
		access_token = rr[-3]
		return access_token
	else: #echec
		print "Authentication Failed"


def extraction(access_token, parametres, request_type):
	"""
	prend en arguments l'access token (string), les parametres (dict) et le type de requete (string) (TODO)
	fait une requete au serveur
	renvoie la reponse de la requete
	"""
	#recherche
	if request_type == "search":
		url = "https://api.twitter.com/1.1/search/tweets.json"
	# TODO a finir
	elif request_type == "TT":
		url = "https://api.twitter.com/1.1/trends/place.json"
	#header d'authentification
	headers = {"Authorization": "Bearer "+access_token}
	#envoi de la requete
	r = requests.get(url, params= parametres, headers=headers)
	#si reussite
	if r.status_code == 429:
		get_ti = requests.get("https://api.twitter.com/1.1/application/rate_limit_status.json", params={"resources":"search"}, headers=headers)
		reset = get_ti.json()["resources"]["search"]["/search/tweets"]["reset"]
		waiting = (reset-time.time())/60
		#print waiting
		print 'Too many requests, please wait %s min.' % (str(waiting))
		sys.exit(1)
	# print r.text.decode('unicode-escape')
	# return r.text.decode('unicode-escape') #Si extraction dans fichier
	return r.json()


def extractTweetsInfos(tweet):
	"""
	prend un tweet au format json en entree (dict)
	traite les donnees du tweet
	retourne retweet (bool) et les colonnes a ecrire dans le fichier de resultats
	"""
	id_str = tweet['id_str']
	user = tweet['user']['name']+' | @'+tweet['user']['screen_name']
	text= tweet['text'].replace('\n',' ')
	if len(tweet['entities']["urls"]) > 0:
		text = text.replace(" ".join(tweet['entities']["urls"][i]['url'] for i in range(len(tweet['entities']["urls"]))), " ".join(tweet['entities']["urls"][i]['expanded_url'] for i in range(len(tweet['entities']["urls"]))))
	hashtags = " ".join([tweet['entities']["hashtags"][i]['text'] for i in range(len(tweet['entities']["hashtags"]))])
	impact = str(tweet['favorite_count']+tweet['retweet_count'])
	creation= tweet["created_at"]
	if 'retweeted_status' in tweet.keys():
		retweet = True
		columns= [id_str,creation,user,text,hashtags,"True",impact]
	else:
		retweet = False
		columns= [id_str,creation,user,text,hashtags,"False",impact]
	return retweet, columns

def writeOutfileColumns(full_data,outfile):
	"""
	prend une reponse de la requete au format json et un handler de fichier de sortie
	traite les tweets un par un en les envoyant a la fonction extract
	ecrit dans le fichier
	"""
	#if "search_metadata" in full_data.keys():
	query=urllib.unquote(full_data["search_metadata"]["query"])
	for tweet in full_data['statuses']:
		retweet, data=extractTweetsInfos(tweet)
		columns=[query]+data
		outfile.write("\t".join(columns)+"\n")
		if retweet:
			retweet, data=extractTweetsInfos(tweet['retweeted_status'])
			columns=[query]+data
			outfile.write("\t".join(columns)+"\n")



def MakeResultsFile(extraction):
	"""
	prend le resultat de l'extraction (list) 
	cree un fichier de sortie et envoie les resultats au traitement
	renvoie le nom du fichier
	"""
	if parametres["q"].startswith("%23"):
		outfilename=parametres["q"][3:]
	else:
		outfilename=parametres["q"]
	outfile = codecs.open(outfilename+"_results.tabular", "w", "utf-8") #on crée le fichier de sortie
	columns=["Infile","query", "id","created_at", "user", "text", "hashtags", "retweet", "impact"]
	outfile.write("\t".join(columns)+"\n")
	print "Ecriture des colonnes"
	#json_data=extraction
	#full_data = json.loads(extraction)
	for e in extraction:
		writeOutfileColumns(e, outfile)
		print "ecriture des donnes"
	outfile.close()
	return outfilename+"_results.tabular"

def fullExtractionData(authentfile,parametres, request_type, restart={False:""}):
	"""
	main function
	prend un dict de parametres de requete, un type de requete (TODO) et un bool pour les requtes incrementales
	fais la requete et cree le fichier de sortie
	imprime le resultat
	"""
	access_token = authentication(authentfile)
	data=extraction(access_token, parametres, request_type)
	if True in restart.keys():
		if restart[True] == "next":
			parametres["since_id"]=data["search_metadata"]#["max_id"]
		elif restart[True] == "prev":
			parametres["max_id"] = data["search_metadata"]["max_id"]
		else:
			print "Please specify chronological order of the search."
			sys.exit(1)
		liste=[]
		i=1
		# ici seulement 10 iterations pour test, max 450
		while i < 10:
			data=extraction(access_token,parametres, request_type)
			liste.append(data)
			i+=1
	name=MakeResultsFile(liste)
	print "Request", request_type, "succeed. Results in file", name+"."


if __name__ == '__main__':
	
	parametres = {"q":"%23Hashtag","lang":"fr","count":100,"result_type":"recent", "since_id":715338749861892099}
	fullExtractionData("credentials.txt",parametres,"search", restart={True:"next"})
	

	#TODO

	# locations = {"Paris":"615702", "Global":"1"}
	# parametres = {"id":"615702"}
	# parametres = {"q":"République","lang":"fr","count":100,"result_type":"recent"}

	# SI TT :
	# parametres = {"id":"YahooIDlocation"}
	# SI search :
	# parametres = {"q":"République","lang":"fr","count":100,"result_type":"recent"}
	# result =extraction(access_token, parametres, "TT")
	# print len(result)

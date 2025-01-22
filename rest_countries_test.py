import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_capitals():
    """
    Interroge l'API REST Countries pour récupérer la liste des capitales de chaque pays du monde.
    Inclut des mécanismes pour limiter les données récupérées et gérer les erreurs de connexion.
    """
    api_url = "https://restcountries.com/v3.1/all?fields=name,capital"

    # Configuration du mécanisme de réessai
    session = requests.Session()
    retries = Retry(
        total=5,  # Nombre total de tentatives
        backoff_factor=0.5,  # Temps d'attente entre les tentatives
        status_forcelist=[500, 502, 503, 504]  # Codes d'erreur qui déclenchent une nouvelle tentative
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        # Effectuer la requête GET avec un timeout
        response = session.get(api_url, timeout=10)
        response.raise_for_status()  # Vérifie si la requête a réussi

        countries = response.json()

        # Extraire les capitales
        capitals = []
        for country in countries:
            name = country.get('name', {}).get('common', 'Unknown Country')
            # Vérifier si la liste des capitales est vide ou non définie
            capital_list = country.get('capital', [])
            capital = capital_list[0] if capital_list else 'No Capital'
            capitals.append((name, capital))

        # Afficher les résultats
        print("Liste des capitales de chaque pays :")
        for name, capital in capitals:
            print(f"{name}: {capital}")

        # Afficher le nombre total de capitales
        print("\nNombre total de capitales capturées :", len(capitals))

    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de l'interrogation de l'API : {e}")

if __name__ == "__main__":
    get_capitals()

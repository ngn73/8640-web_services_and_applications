import requests
import csv
from xml.dom.minidom import parseString

baseline_url = "https://deckofcardsapi.com/api/deck/"
number_of_decks = 1 # Blackjack typically uses 6 decks. The default is 1.
number_of_cards = 2 # Number of cards to draw
cards = []

def shuffle_cards()->str:
    # Shuffle the Cards: Add deck_count as a GET or POST parameter to define the number of Decks you want to use.
    url = f"{baseline_url}/new/shuffle/?deck_count={number_of_decks}"
    r = requests.get(url)
    data = r.json()
    return data["deck_id"]

def draw_cards(deck_id:str, count:int)->list:
    # Draw Cards: Use the draw endpoint to draw cards from the deck. You can specify how many cards you want to draw.
    url = f"{baseline_url}/{deck_id}/draw/?count={count}"
    r = requests.get(url)
    data = r.json()
    return data["cards"]

def process_cards(cards:list)->None:
    # Process the Cards: You can process the drawn cards as needed. For example, you can calculate the total value of the hand in Blackjack.
    total_value = 0
    print(f"Total value of hand: {total_value}")

if __name__ == "__main__":
    # Shuffle cards
    deck_id = shuffle_cards()
    # Draw cards
    drawn_cards = draw_cards(deck_id, number_of_cards) # Draw cards
    
    # Add the drawn cards to a list
    for idx in range(number_of_cards):
        card = {
            "value": drawn_cards[idx]['value'],
            "suit": drawn_cards[idx]['suit'],
            "image": drawn_cards[idx]['image']
        }
        cards.append(card)
        print(f"Card {idx+1}: {card['value']} of {card['suit']}")
    

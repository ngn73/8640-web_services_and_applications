import requests
import csv
from xml.dom.minidom import parseString
from collections import defaultdict

baseline_url = "https://deckofcardsapi.com/api/deck/"
number_of_decks = 1 # Blackjack typically uses 6 decks. The default is 1.
number_of_cards = 5 # Number of cards to draw
cards = []
RANK_TO_INT = {
    "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7,
    "8": 8, "9": 9, "10": 10,
    "JACK": 11, "QUEEN": 12, "KING": 13, "ACE": 14
}

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

def process_cards(cards:list)->list:
    results = []
    # Evaluate the Drawn Cards: After drawing the cards, you can evaluate them to determine if you have a pair, three of a kind, etc. You can use the value and suit of the cards to make these determinations.
    rank_counts = defaultdict(int)
    sorted_ranks = sorted(RANK_TO_INT[card["value"]] for card in cards) #Sorted Ranks needed to evaluate straights and convert the card values to integers
    for r in sorted_ranks:
        rank_counts[r] += 1

    suit_counts = defaultdict(int)
    suits = [card['suit'] for card in cards]
    for s in suits:
        suit_counts[s] += 1

    # Test for pairs, three of a kind, four of a kind
    has_pair = 2 in rank_counts.values()
    has_triple = 3 in rank_counts.values()
    has_quad = 4 in rank_counts.values()
    # Test for same suit (flush)
    same_suit = False
    if number_of_cards > 4:
        same_suit = 4 in suit_counts.values()
    else:
        same_suit = number_of_cards in suit_counts.values()
    # a Straight is bit more complex to evaluate
    is_straight = False
    if len(set(sorted_ranks)) == number_of_cards and (sorted_ranks[-1] - sorted_ranks[0]) == number_of_cards - 1:
        is_straight = True
    
    
    if(has_quad):
        results.append("Found a four of a kind!")
    elif(has_triple):
        results.append("Found a three of a kind!")
    elif(has_pair):
        results.append("Found a pair!")
    if(same_suit):
        results.append("All cards are of the same suit!")
    if(is_straight):
        results.append("Found a straight!")
    
    return results


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
    

    hand_result = process_cards(cards)
    for result in hand_result:  # If any of the hand results are true, print the result
        print(result)

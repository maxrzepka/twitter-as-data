import unittest

from keyword_map import extract

CORRECT_EXTRACTION = [
        ["Suspended kami! Woop! :))) HAHA. NBA Finals. Yesss!"
            ,"nba"]
        ,["Ready for the spurs to win game 7!","spurs"]
        ,["@KingJames better not wear his headband tonight.","kingjames"]
        ,["RT @MySportsLegion: LeBron James averages 33.8 points a game in Game 7s in his career. That's the highest in NBA History.","nba"]
        ,["RT @HoustonTaxi: Whether you're a #Heat or #Spurs Fan, @HailaCabApp to @CycloneAnayas for the @NBA #Finals #Game7 tonight @ 8pm!","game7 nba spurs"]
        ]

class TestExtract(unittest.TestCase):

    def setUp(self):
        self.keywords = set(map(lambda s: s.lower(),['MiamiiHeatGang', 'miamiheat', 'spurs', 'nbafinals', 'nba', 'basketball', 'ginobili', 'DwyaneWade', 'tonyparker', 'kingjames', 'nbastats', 'game7']))

    def test(self):
        all_ks = [extract(text,self.keywords)[1] for text,correct_ks in CORRECT_EXTRACTION]
        correct_ks = [a[1] for a in CORRECT_EXTRACTION]
        self.assertEqual(all_ks,correct_ks) 

if __name__ == '__main__':
    unittest.main()


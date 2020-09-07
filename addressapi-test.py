import unittest
import flask

from addressapi import app
from include import utils
import settings




class AddressAPITestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        # @TODO: http://dev.colpo.net:8888/api/activity?type=bitcoin&address=1KAD5EnzzLtrSo2Da2G4zzD7uZrjk8zRAv
        # Shows vout unspent with balance, but overall has 0 balance
        self.balances = {
            'bitcoin': {
                '1C1ENNWdkPMyhZ7xTEM4Kwq1FTUifZNCRd': 5000000000,
                '1dyoBoF5vDmPCxwSsUZbbYhA5qjAfBTx9': 5000000000,
                '16LoW7y83wtawMg5XmT4M3Q7EdjjUmenjM': 5002000000,
                '1KAD5EnzzLtrSo2Da2G4zzD7uZrjk8zRAv': 0,
                '1AcvGkfoRtujPRN85YSiGKTs6EnFnw5Wtk': 2705333333,
                '1PybqAMaavVbejrTWSrcq5eYuRbx1MrYMz': 0,
                '13JM4mHbeqFgnE6woSVPxPWMHZwhNmN5ui': 0,
                '1LJQZAecDMnhQs8eJHjQ7Zsak6cre1HvqG': 712617,
                '16iGsinQs4MtTzGJJcKv1pHu88ZvvFuc1Z': 366064,
                '1QKqcixDqgiLK7Jh4cpxgndqdiPd1M72XU': 45000000,
                '19Fj4drFPPKisc45V4u8dGnjxkQKmJx8mi': 0,
            }
        }

    def test_no_type_or_address(self):
        response = self.app.get('/api/address/')
        assert response
        data = flask.json.loads(response.data)
        assert data['status_code'] == 400
        assert data['error'] == 'type is required'
        assert data['status'] == 'Bad Request'
        assert data['type'] == None

    def test_invalid_type(self):
        response = self.app.get('/api/address/nosuchtype/abcdefabcdefabcdef')
        assert response
        data = flask.json.loads(response.data)
        assert data['status_code'] == 400
        assert data['details'].startswith('must be one of')
        assert data['error'] == 'unrecognized coin type'
        assert data['status'] == 'Bad Request'
        assert data['type'] == 'nosuchtype'

    def test_no_address(self):
        for coin in utils.supported_coins(settings):
            response = self.app.get('/api/address/%s' % (coin,))
            assert response
            data = flask.json.loads(response.data)
            assert data['status_code'] == 400
            assert data['address'] == None
            assert data['error'] == 'address is required'
            assert data['status'] == 'Bad Request'
            assert data['type'] == coin

    def test_invalid_address(self):
        address = 'zz339'
        for coin in utils.supported_coins(settings):
            response = self.app.get('/api/address/%s/%s' % (coin, address))
            assert response
            data = flask.json.loads(response.data)
            try:
                assert data['status_code'] == 400
            except:
                if 'error' in data:
                    print("\n%s rpc: %s != 400 (%s)" % (coin, data['status_code'], data['error']))
                else:
                    print("\n%s rpc: %s != 400" % (coin, data['status_code']))
                exit(1)
            assert data['address'] == address
            assert data['error'] == 'address is invalid'
            assert data['status'] == 'Bad Request'
            assert data['type'] == coin

    def test_addresses(self):
        for coin in self.balances:
            for address in self.balances[coin]:
                response = self.app.get('/api/address/%s/%s' % (coin, address))
                assert response
                data = flask.json.loads(response.data)
                assert data['status_code'] == 200
                assert data['address_details']['isvalid'] == True
                assert data['status'] == 'OK'
                assert data['address'] == address
                '''
                @TODO: need to fix balance
                try:
                    assert data['balance'] == self.balances[coin][address]
                except:
                    print("\nbalance[%s]: %s != %s" % (address, data['balance'], self.balances[coin][address]))
                    exit(1)
                    '''

if __name__ == '__main__':
    unittest.main()
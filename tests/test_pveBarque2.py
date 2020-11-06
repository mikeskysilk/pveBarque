import unittest
from pveBarque.pveBarque import pveBarque


class FlaskTestCase(unittest.TestCase):

    def test_example(self):
        tester = pveBarque.app.test_client(self)
        response = tester.get("/barque/info")
        self.assertEqual(response.status_code, 401)

if __name__ == "__main__":
    unittest.main()

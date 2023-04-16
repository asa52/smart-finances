"""Test functions in apis.py"""

from src.apis import url_response


def test_url_response():
    """URL response to Government inflation tracker API succeeds."""
    url = 'https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflation' \
          'andpriceindices/timeseries/l55o/mm23'
    assert(url_response(url).status_code == 200)


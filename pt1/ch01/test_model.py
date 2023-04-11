from model import Money, Name, Line


def test_equality():
    assert Money('gbp', 10) == Money('gbp', 10)
    assert Name('Seongeun', 'Yu') != Name('Junai', 'Sakura')
    assert Line('RED-CHAIR', 5) == Line('RED-CHAIR', 5)

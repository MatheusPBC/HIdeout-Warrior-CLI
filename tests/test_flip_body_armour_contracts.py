from core.market_scanner import ScanOpportunity


def test_scan_opportunity_accepts_body_armour_structure_fields():
    opportunity = ScanOpportunity(
        item_id="x",
        base_type="Sadist Garb",
        item_family="body_armour_defense",
        ilvl=84,
        listed_price=40.0,
        ml_value=90.0,
        ml_confidence=0.8,
        profit=50.0,
        score=80.0,
        valuation_gap=50.0,
        relative_discount=0.55,
        whisper="@seller",
        trade_link="https://trade/1",
        trade_search_link="https://trade/search/1",
        listing_currency="chaos",
        listing_amount=40.0,
        seller="seller",
        indexed_at="2026-03-11T10:00:00Z",
        resolved_league="Mirage",
        corrupted=False,
        fractured=False,
        defence_profile="evasion_energy_shield",
        attribute_profile="dex_int",
        socket_count=6,
        link_count=6,
        socket_colour_profile="G:3,R:2,B:1",
    )

    assert opportunity.defence_profile == "evasion_energy_shield"
    assert opportunity.link_count == 6

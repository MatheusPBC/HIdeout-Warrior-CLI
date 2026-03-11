from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

if TYPE_CHECKING:
    from core.graph_engine import ItemState

ITEM_FAMILIES = (
    "wand_caster",
    "body_armour_defense",
    "jewel_cluster",
    "accessory_generic",
    "generic",
)


@dataclass(frozen=True)
class NormalizedMarketItem:
    item_id: str
    base_type: str
    item_family: str
    ilvl: int
    listed_price: float
    listing_currency: str
    listing_amount: float
    seller: str
    listed_at: Optional[str]
    whisper: str
    corrupted: bool
    fractured: bool
    influences: List[str] = field(default_factory=list)
    explicit_mods: List[str] = field(default_factory=list)
    implicit_mods: List[str] = field(default_factory=list)
    prefix_count: int = 0
    suffix_count: int = 0
    open_prefixes: int = 3
    open_suffixes: int = 3
    mod_tokens: List[str] = field(default_factory=list)
    tag_tokens: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_item_state(self) -> "ItemState":
        from core.graph_engine import ItemState

        prefix_tokens = frozenset(self.mod_tokens[: self.prefix_count])
        suffix_tokens = frozenset(
            self.mod_tokens[self.prefix_count : self.prefix_count + self.suffix_count]
        )
        return ItemState(
            base_type=self.base_type,
            ilvl=self.ilvl,
            prefixes=prefix_tokens,
            suffixes=suffix_tokens,
            is_fractured=bool(self.fractured or self.influences),
        )


@dataclass(frozen=True)
class ComparableMarketStats:
    market_floor: float
    market_median: float
    market_spread: float
    comparables_count: int
    pricing_position: str

    def to_dict(self) -> dict:
        return asdict(self)


def _normalize_mod_text(mod: str) -> str:
    return mod.lower().replace("%", " percent ").replace("+", " ")


def _extract_mod_tokens(explicit_mods: Iterable[str], implicit_mods: Iterable[str]) -> List[str]:
    tokens: List[str] = []
    for mod in list(explicit_mods) + list(implicit_mods):
        text = _normalize_mod_text(mod)
        if "spell" in text and "damage" in text:
            tokens.append("SpellDamage1")
        if "cast speed" in text or "casting speed" in text:
            tokens.append("CastSpeed1")
        if "critical" in text and "spell" in text:
            tokens.append("CritChanceSpells1")
        if "maximum life" in text or " life" in text:
            tokens.append("Life1")
        if "suppress" in text:
            tokens.append("SpellSuppress1")
        if "resist" in text or "resistance" in text:
            tokens.append("Resist1")
        if "cluster" in text:
            tokens.append("ClusterPassive1")
        if "mana" in text:
            tokens.append("Mana1")
        if "attributes" in text or "strength" in text or "dexterity" in text or "intelligence" in text:
            tokens.append("Attributes1")
        if "chaos" in text:
            tokens.append("Chaos1")
    return list(dict.fromkeys(tokens))


def _extract_tag_tokens(base_type: str, mods: Iterable[str], influences: Iterable[str]) -> List[str]:
    tags: List[str] = []
    base_lower = base_type.lower()
    if "wand" in base_lower:
        tags.append("wand")
        tags.append("caster")
    if "armour" in base_lower or "garb" in base_lower or "regalia" in base_lower:
        tags.append("body_armour")
    if "jewel" in base_lower:
        tags.append("jewel")
    if "ring" in base_lower or "amulet" in base_lower or "belt" in base_lower:
        tags.append("accessory")

    for mod in mods:
        text = _normalize_mod_text(mod)
        if "spell" in text:
            tags.append("spell")
        if "attack" in text:
            tags.append("attack")
        if "life" in text:
            tags.append("life")
        if "resist" in text:
            tags.append("resistance")
        if "suppress" in text:
            tags.append("suppression")
        if "critical" in text or "crit" in text:
            tags.append("crit")

    for influence in influences:
        tags.append(influence.lower())

    return list(dict.fromkeys(tags))


def classify_item_family(base_type: str, tag_tokens: Iterable[str]) -> str:
    base_lower = base_type.lower()
    tags = set(tag_tokens)
    if "wand" in base_lower or {"wand", "caster"}.issubset(tags):
        return "wand_caster"
    if "jewel" in base_lower:
        return "jewel_cluster"
    if any(token in base_lower for token in ("ring", "amulet", "belt")):
        return "accessory_generic"
    if any(token in base_lower for token in ("armour", "garb", "regalia")):
        return "body_armour_defense"
    return "generic"


def _count_affixes(mod_tokens: List[str], explicit_mods: List[str]) -> tuple[int, int]:
    explicit_count = min(len(explicit_mods), 6)
    if explicit_count <= 0:
        return (0, 0)

    # Balanced split is more stable than alternating parser heuristics.
    prefix_count = min(3, (explicit_count + 1) // 2)
    suffix_count = min(3, explicit_count - prefix_count)

    if prefix_count + suffix_count < explicit_count:
        suffix_count = min(3, explicit_count - prefix_count)

    if prefix_count + suffix_count > len(mod_tokens):
        suffix_count = max(0, len(mod_tokens) - prefix_count)

    return (prefix_count, suffix_count)


def normalize_trade_item(
    item_json: Dict[str, Any],
    listed_price: float,
    listing_currency: str,
    listing_amount: float,
) -> Optional[NormalizedMarketItem]:
    listing = item_json.get("listing", {})
    item_data = item_json.get("item", {})
    whisper = listing.get("whisper", "")
    if not item_data or not whisper:
        return None

    explicit_mods = list(item_data.get("explicitMods", []) or [])
    implicit_mods = list(item_data.get("implicitMods", []) or [])
    influences = list((item_data.get("influences", {}) or {}).keys())
    mod_tokens = _extract_mod_tokens(explicit_mods, implicit_mods)
    tag_tokens = _extract_tag_tokens(
        item_data.get("baseType", "Unknown Base"),
        explicit_mods + implicit_mods,
        influences,
    )
    item_family = classify_item_family(item_data.get("baseType", ""), tag_tokens)
    prefix_count, suffix_count = _count_affixes(mod_tokens, explicit_mods)

    return NormalizedMarketItem(
        item_id=item_data.get("id", ""),
        base_type=item_data.get("baseType", "Unknown Base"),
        item_family=item_family,
        ilvl=int(item_data.get("ilvl", 1) or 1),
        listed_price=round(float(listed_price), 1),
        listing_currency=listing_currency or "chaos",
        listing_amount=float(listing_amount or 0.0),
        seller=listing.get("account", {}).get("name", ""),
        listed_at=listing.get("indexed") or None,
        whisper=whisper,
        corrupted=bool(item_data.get("corrupted", False)),
        fractured=bool(item_data.get("fractured", False)),
        influences=influences,
        explicit_mods=explicit_mods,
        implicit_mods=implicit_mods,
        prefix_count=prefix_count,
        suffix_count=suffix_count,
        open_prefixes=max(0, 3 - prefix_count),
        open_suffixes=max(0, 3 - suffix_count),
        mod_tokens=mod_tokens,
        tag_tokens=tag_tokens,
    )


def normalized_item_from_item_state(item_state: "ItemState") -> NormalizedMarketItem:
    mod_tokens = list(item_state.prefixes) + list(item_state.suffixes)
    tag_tokens = _extract_tag_tokens(item_state.base_type, mod_tokens, [])
    item_family = classify_item_family(item_state.base_type, tag_tokens)
    return NormalizedMarketItem(
        item_id="",
        base_type=item_state.base_type,
        item_family=item_family,
        ilvl=item_state.ilvl,
        listed_price=0.0,
        listing_currency="chaos",
        listing_amount=0.0,
        seller="",
        listed_at=None,
        whisper="",
        corrupted=False,
        fractured=bool(item_state.is_fractured),
        influences=[],
        explicit_mods=mod_tokens,
        implicit_mods=[],
        prefix_count=len(item_state.prefixes),
        suffix_count=len(item_state.suffixes),
        open_prefixes=item_state.open_prefixes,
        open_suffixes=item_state.open_suffixes,
        mod_tokens=mod_tokens,
        tag_tokens=tag_tokens,
    )


def build_comparable_market_stats(
    item: NormalizedMarketItem,
    comparable_prices: List[float],
) -> ComparableMarketStats:
    prices = sorted(float(price) for price in comparable_prices if price > 0)
    if not prices:
        return ComparableMarketStats(
            market_floor=item.listed_price,
            market_median=item.listed_price,
            market_spread=0.0,
            comparables_count=1,
            pricing_position="near_market",
        )

    market_floor = round(prices[0], 1)
    middle = len(prices) // 2
    if len(prices) % 2 == 0:
        market_median = round((prices[middle - 1] + prices[middle]) / 2, 1)
    else:
        market_median = round(prices[middle], 1)
    market_spread = round(max(prices) - min(prices), 1)

    if item.listed_price <= max(1.0, market_floor * 0.98):
        pricing_position = "below_floor"
    elif item.listed_price > max(market_median * 1.35, market_floor + max(market_spread, 10.0)):
        pricing_position = "outlier"
    else:
        pricing_position = "near_market"

    return ComparableMarketStats(
        market_floor=market_floor,
        market_median=market_median,
        market_spread=market_spread,
        comparables_count=len(prices),
        pricing_position=pricing_position,
    )


"""
Meta Analyzer Module for Hideout Warrior CLI.

This module provides ladder analysis capabilities by fetching build data from poe.ninja,
analyzing skill and keystone popularity, and mapping them to item tags for meta-scoring.
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field

import requests
import numpy as np


@dataclass
class MetaScores:
    """Container for meta scores mapped to game tags."""
    scores: Dict[str, float] = field(default_factory=dict)
    last_updated: Optional[datetime] = None
    
    def get_score(self, tag: str, default: float = 0.0) -> float:
        """Get score for a specific tag, returning default if not found."""
        return self.scores.get(tag.lower(), default)
    
    def is_fresh(self, max_age_hours: float = 4.0) -> bool:
        """Check if the scores are still fresh based on age."""
        if self.last_updated is None:
            return False
        age = datetime.now() - self.last_updated
        return age < timedelta(hours=max_age_hours)


class LadderAnalyzer:
    """
    Analyzes ladder data from poe.ninja to determine meta weights.
    
    Fetches build data, analyzes skill and keystone frequencies, and maps
    them to item tags with normalized weights (0.0-1.0).
    
    Attributes:
        league: The PoE league to analyze (default: "Standard")
        cache_file: Path to local cache file for meta weights
        cache_duration_hours: How long to keep cached results
        _meta_scores: Cached MetaScores instance
    """
    
    # Skill to tag mappings based on common PoE archetypes
    SKILL_TAG_MAPPINGS: Dict[str, List[str]] = {
        # Fire skills
        "righteous fire": ["fire", "life", "regen", "burning", "area"],
        "fireball": ["fire", "spell", "casting"],
        "infernal blow": ["fire", "attack", "melee"],
        "flameblast": ["fire", "spell", "channeling", "area"],
        "incinerate": ["fire", "spell", "channeling"],
        
        # Cold skills
        "ice shot": ["cold", "attack", "bow", "projectile"],
        "freezing pulse": ["cold", "spell", "projectile"],
        "ice trap": ["cold", "trap", "spell"],
        "frost blades": ["cold", "attack", "melee", "projectile"],
        "vortex": ["cold", "spell", "area", "dot"],
        
        # Lightning skills
        "lightning arrow": ["lightning", "attack", "bow", "projectile"],
        "arc": ["lightning", "spell", "chaining"],
        "ball lightning": ["lightning", "spell", "area"],
        "spark": ["lightning", "spell", "projectile"],
        "storm brand": ["lightning", "spell", "brand", "activation"],
        "lightning strike": ["lightning", "attack", "melee", "projectile"],
        
        # Physical/Chaos skills
        "blade vortex": ["physical", "spell", "area"],
        "blade flurry": ["physical", "attack", "melee", "channeling"],
        "toxic rain": ["chaos", "attack", "bow", "dot"],
        "caustic arrow": ["chaos", "attack", "bow", "dot", "area"],
        "viper strike": ["chaos", "attack", "melee", "poison"],
        
        # Minion skills
        "raise zombie": ["minion", "life", "physical"],
        "raise spectre": ["minion", "spell", "life"],
        "summon skeletons": ["minion", "physical", "life"],
        "dominating blow": ["minion", "attack", "melee"],
        " Herald of Purity": ["minion", "physical", "herald"],
        
        # Aura/Buff focused
        "herald of ice": ["cold", "herald", "crit", "shield"],
        "herald of thunder": ["lightning", "herald", "shock", "mana"],
        "herald of ash": ["fire", "herald", "burning"],
        "determination": ["armor", "defense", "life"],
        "grace": ["evasion", "defense", "life"],
        "discipline": ["energy_shield", "defense", "life"],
        
        # Crit-focused
        "power siphon": ["lightning", "attack", "wand", "crit", "power_charge"],
        "assassins mark": ["crit", "curse", "power_charge"],
        
        # Totem/Mine/Trap
        "ancestral warchief": ["totem", "attack", "melee", "area"],
        "ice trap": ["trap", "cold", "spell"],
        "arc trap": ["trap", "lightning", "spell"],
    }
    
    # Keystone to tag mappings
    KEYSTONE_TAG_MAPPINGS: Dict[str, List[str]] = {
        # Life/Defense keystones
        "vaal pact": ["life", "leech", "regen", "defense"],
        "blood magic": ["life", "mana", "cost", "life_cost"],
        "resolute technique": ["accuracy", "crit", "attack"],
        "iron reflexes": ["evasion", "armor", "defense"],
        "chaos inoculation": ["energy_shield", "chaos", "defense", "life"],
        "eldrich battery": ["energy_shield", "mana", "defense"],
        "mind over matter": ["mana", "life", "defense"],
        "arrow dancing": ["evasion", "ranged", "defense"],
        
        # Damage keystones
        "ancestral bond": ["totem", "damage", "spell"],
        "avatar of fire": ["fire", "conversion", "elemental"],
        "runebinder": ["brand", "damage", "attachment"],
        "hex master": ["curse", "duration", "aoe"],
        "elemental equilibrium": ["resistance", "elemental", "damage"],
        "elemental overload": ["elemental", "crit", "damage"],
        "ghost reaver": ["energy_shield", "leech", "life", "defense"],
        "point blank": ["projectile", "damage", "ranged", "bow"],
        "crimson dance": ["bleed", "physical", "damage", "attack"],
        "perfect agony": ["poison", "damage", "ailment", "crit"],
        
        # Minion keystones
        "spiritual aid": ["minion", "damage", "aura"],
        "spiritual command": ["minion", "speed", "attack", "cast"],
        
        # Crit keystones  
        "cast when damage taken": ["trigger", "defense", "automation"],
        "acrobatics": ["evasion", "dodge", "defense", "armor", "block"],
        "phase acrobatics": ["evasion", "dodge", "defense", "spell"],
        
        # Core attribute scaling
        "iron grip": ["strength", "projectile", "damage", "attack"],
        "iron will": ["strength", "spell", "damage", "cast"],
    }
    
    def __init__(
        self,
        league: str = "Standard",
        cache_file: str = "data/meta_scores_cache.json",
        cache_duration_hours: float = 4.0,
        user_agent: str = "HideoutWarrior-CLI/1.0"
    ):
        """
        Initialize the LadderAnalyzer.
        
        Args:
            league: The PoE league to analyze
            cache_file: Path to local cache file
            cache_duration_hours: How long to cache results
            user_agent: User agent for API requests
        """
        self.league = league
        self.cache_file = cache_file
        self.cache_duration_hours = cache_duration_hours
        self.user_agent = user_agent
        self._meta_scores: Optional[MetaScores] = None
        self._api_url = "https://poe.ninja/api/data/builds"
        
        # Ensure cache directory exists
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    
    def fetch_meta_weights(
        self,
        force_refresh: bool = False,
        sample_size: int = 500
    ) -> MetaScores:
        """
        Fetch and analyze meta weights from poe.ninja ladder data.
        
        Makes a GET request to poe.ninja builds API, analyzes skill and keystone
        frequencies, and maps them to item tags with normalized weights.
        
        Args:
            force_refresh: If True, bypass cache and fetch fresh data
            sample_size: Number of builds to analyze from ladder
            
        Returns:
            MetaScores object containing tag weights (0.0-1.0)
            
        Raises:
            requests.RequestException: If API request fails (after retries)
        """
        # Check cache first
        if not force_refresh:
            cached = self._load_from_cache()
            if cached and cached.is_fresh(self.cache_duration_hours):
                self._meta_scores = cached
                return cached
        
        try:
            data = self._fetch_ladder_data()
            builds = data.get("builds", [])
            
            if not builds:
                # Return empty scores if no builds found
                return self._create_empty_scores()
            
            # Sample builds if we have more than requested
            if len(builds) > sample_size:
                import random
                builds = random.sample(builds, sample_size)
            
            # Analyze frequencies
            tag_frequencies = self._analyze_builds(builds)
            
            # Normalize to 0.0-1.0 range
            scores = self._normalize_scores(tag_frequencies)
            
            # Create MetaScores
            meta_scores = MetaScores(
                scores=scores,
                last_updated=datetime.now()
            )
            
            # Cache results
            self._save_to_cache(meta_scores)
            self._meta_scores = meta_scores
            
            return meta_scores
            
        except requests.RequestException as e:
            # Try to return stale cache if available
            stale_cache = self._load_from_cache()
            if stale_cache:
                print(f"⚠️ [LadderAnalyzer] API failed, using stale cache: {e}")
                return stale_cache
            
            # Return empty scores as fallback
            print(f"⚠️ [LadderAnalyzer] API failed, no cache available: {e}")
            return self._create_empty_scores()
        except Exception as e:
            print(f"⚠️ [LadderAnalyzer] Unexpected error: {e}")
            return self._create_empty_scores()
    
    def _fetch_ladder_data(self) -> Dict[str, Any]:
        """
        Fetch raw ladder data from poe.ninja API.
        
        Returns:
            Raw JSON response from API
            
        Raises:
            requests.RequestException: On network/API errors
        """
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json"
        }
        
        params = {
            "league": self.league
        }
        
        try:
            response = requests.get(
                self._api_url,
                headers=headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
            
        except requests.Timeout:
            raise requests.RequestException("Request timed out after 30s")
        except requests.HTTPError as e:
            raise requests.RequestException(f"HTTP error: {e.response.status_code}")
        except requests.ConnectionError:
            raise requests.RequestException("Connection error")
    
    def _analyze_builds(self, builds: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Analyze builds to count tag frequencies.
        
        Args:
            builds: List of build dictionaries from API
            
        Returns:
            Dictionary mapping tags to frequency counts
        """
        tag_counts: Dict[str, int] = {}
        
        for build in builds:
            # Extract skills
            skills = build.get("skills", [])
            if isinstance(skills, list):
                for skill in skills:
                    skill_name = skill.get("name", "").lower() if isinstance(skill, dict) else str(skill).lower()
                    tags = self._get_skill_tags(skill_name)
                    for tag in tags:
                        tag_counts[tag] = tag_counts.get(tag, 0) + 1
            
            # Extract keystones (from tree or keystone field)
            keystones: Set[str] = set()
            
            # Direct keystone field
            keystone = build.get("keystone")
            if keystone:
                if isinstance(keystone, str):
                    keystones.add(keystone.lower())
                elif isinstance(keystone, list):
                    keystones.update(k.lower() for k in keystone if isinstance(k, str))
            
            # From passive tree
            tree = build.get("tree", {})
            if isinstance(tree, dict):
                tree_keystones = tree.get("keystones", [])
                if isinstance(tree_keystones, list):
                    keystones.update(k.lower() for k in tree_keystones if isinstance(k, str))
            
            # Count tags for keystones
            for keystone_name in keystones:
                tags = self._get_keystone_tags(keystone_name)
                for tag in tags:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        return tag_counts
    
    def _get_skill_tags(self, skill_name: str) -> List[str]:
        """Get tags associated with a skill."""
        skill_name = skill_name.lower().strip()
        
        # Direct lookup
        if skill_name in self.SKILL_TAG_MAPPINGS:
            return self.SKILL_TAG_MAPPINGS[skill_name]
        
        # Partial match
        for known_skill, tags in self.SKILL_TAG_MAPPINGS.items():
            if known_skill in skill_name or skill_name in known_skill:
                return tags
        
        return []
    
    def _get_keystone_tags(self, keystone_name: str) -> List[str]:
        """Get tags associated with a keystone."""
        keystone_name = keystone_name.lower().strip()
        
        # Direct lookup
        if keystone_name in self.KEYSTONE_TAG_MAPPINGS:
            return self.KEYSTONE_TAG_MAPPINGS[keystone_name]
        
        # Partial match
        for known_keystone, tags in self.KEYSTONE_TAG_MAPPINGS.items():
            if known_keystone in keystone_name or keystone_name in known_keystone:
                return tags
        
        return []
    
    def _normalize_scores(self, tag_counts: Dict[str, int]) -> Dict[str, float]:
        """
        Normalize tag counts to 0.0-1.0 range.
        
        Uses min-max normalization with a floor of 0.1 for any tag
        that appears at least once.
        
        Args:
            tag_counts: Raw frequency counts
            
        Returns:
            Normalized scores dictionary
        """
        if not tag_counts:
            return {}
        
        max_count = max(tag_counts.values())
        min_count = min(tag_counts.values())
        
        if max_count == min_count:
            # All same frequency, give them all 0.5
            return {tag: 0.5 for tag in tag_counts}
        
        scores = {}
        for tag, count in tag_counts.items():
            # Min-max normalization
            normalized = (count - min_count) / (max_count - min_count)
            # Ensure minimum visibility for any present tag
            scores[tag] = max(0.1, round(normalized, 3))
        
        return scores
    
    def _create_empty_scores(self) -> MetaScores:
        """Create empty MetaScores for fallback."""
        return MetaScores(scores={}, last_updated=datetime.now())
    
    def _load_from_cache(self) -> Optional[MetaScores]:
        """Load MetaScores from cache file if it exists."""
        try:
            if not os.path.exists(self.cache_file):
                return None
            
            with open(self.cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Parse timestamp
            timestamp_str = data.get("last_updated")
            last_updated = None
            if timestamp_str:
                try:
                    last_updated = datetime.fromisoformat(timestamp_str)
                except (ValueError, TypeError):
                    pass
            
            return MetaScores(
                scores=data.get("scores", {}),
                last_updated=last_updated
            )
            
        except (json.JSONDecodeError, IOError, KeyError) as e:
            print(f"⚠️ [LadderAnalyzer] Failed to load cache: {e}")
            return None
    
    def _save_to_cache(self, meta_scores: MetaScores) -> None:
        """Save MetaScores to cache file."""
        try:
            data = {
                "scores": meta_scores.scores,
                "last_updated": meta_scores.last_updated.isoformat() if meta_scores.last_updated else None,
                "league": self.league
            }
            
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
                
        except IOError as e:
            print(f"⚠️ [LadderAnalyzer] Failed to save cache: {e}")
    
    def get_cached_scores(self) -> Optional[MetaScores]:
        """Get cached scores without fetching."""
        if self._meta_scores is not None:
            return self._meta_scores
        return self._load_from_cache()
    
    def clear_cache(self) -> None:
        """Clear the local cache file."""
        try:
            if os.path.exists(self.cache_file):
                os.remove(self.cache_file)
                self._meta_scores = None
        except IOError as e:
            print(f"⚠️ [LadderAnalyzer] Failed to clear cache: {e}")


def calculate_meta_utility_score(
    item_tags: List[str],
    meta_scores: MetaScores,
    aggregation: str = "mean"
) -> float:
    """
    Calculate meta utility score for an item based on its tags.
    
    Args:
        item_tags: List of tags associated with the item
        meta_scores: MetaScores containing current meta weights
        aggregation: How to aggregate tag scores ("mean", "max", "sum")
        
    Returns:
        Meta utility score (0.0-1.0)
    """
    if not item_tags or not meta_scores.scores:
        return 0.0
    
    scores = []
    for tag in item_tags:
        score = meta_scores.get_score(tag.lower(), 0.0)
        if score > 0:
            scores.append(score)
    
    if not scores:
        return 0.0
    
    if aggregation == "mean":
        return round(np.mean(scores), 3)
    elif aggregation == "max":
        return round(max(scores), 3)
    elif aggregation == "sum":
        # Normalize sum by number of tags to keep in 0-1 range
        return round(min(sum(scores) / len(scores), 1.0), 3)
    else:
        return round(np.mean(scores), 3)


# Default instance for module-level usage
default_analyzer = LadderAnalyzer()


def get_current_meta_scores(
    league: str = "Standard",
    force_refresh: bool = False
) -> MetaScores:
    """
    Convenience function to get current meta scores.
    
    Args:
        league: PoE league to analyze
        force_refresh: Force API refresh
        
    Returns:
        MetaScores with current tag weights
    """
    analyzer = LadderAnalyzer(league=league)
    return analyzer.fetch_meta_weights(force_refresh=force_refresh)

"""
Smart cast filtering for different types of reality TV shows.
"""

def get_show_category(show_name):
    """Categorize show type for appropriate filtering rules."""
    show_lower = show_name.lower()
    
    # Celebrity episodic shows - 1 episode is normal
    celebrity_episodic_keywords = [
        'celebrity family feud',
        'wife swap', 
        'celebrity wife swap',
        'stars on mars',
        'celebrity big brother', # Special case - short season format
        'celebrity apprentice',
        'dancing with the stars',
        'masked singer'
    ]
    
    # Competition reality - need 2+ episodes to be notable
    competition_keywords = [
        'survivor',
        'big brother', 
        'amazing race',
        'bachelor',
        'bachelorette',
        'love island',
        'traitors',
        'challenge',
        'real world',
        'are you the one',
        'temptation island',
        'too hot to handle'
    ]
    
    # Reality series - variable rules
    reality_series_keywords = [
        'housewives',
        'kardashian',
        'below deck',
        'vanderpump',
        'southern charm',
        'summer house',
        'jersey shore'
    ]
    
    for keyword in celebrity_episodic_keywords:
        if keyword in show_lower:
            return 'celebrity_episodic'
    
    for keyword in competition_keywords:
        if keyword in show_lower:
            # Special case: Celebrity Big Brother is episodic format
            if 'celebrity big brother' in show_lower:
                return 'celebrity_episodic'
            return 'competition'
    
    for keyword in reality_series_keywords:
        if keyword in show_lower:
            return 'reality_series'
    
    return 'other'

def should_include_cast_member(name, imdb_id, show_name, episodes, seasons=None):
    """
    Determine if a cast member should be included based on show type and participation.
    
    Args:
        name: Cast member name
        imdb_id: IMDb ID  
        show_name: Show name
        episodes: Number of episodes (can be None)
        seasons: Number of seasons (can be None)
        
    Returns:
        (should_include: bool, reason: str)
    """
    show_category = get_show_category(show_name)
    
    # Handle missing episode data
    if episodes is None:
        episodes = 0
    if seasons is None:
        seasons = 0
    
    if show_category == 'celebrity_episodic':
        # For celebrity shows, 1 episode is normal and notable
        if episodes >= 1 or seasons >= 1:
            return True, f"Celebrity show participant ({episodes} eps)"
        else:
            return False, f"No episodes recorded for celebrity show"
    
    elif show_category == 'competition':
        # For competition shows, need 2+ episodes to be notable  
        if episodes >= 2:
            return True, f"Competition show participant ({episodes} eps, 2+ threshold)"
        elif seasons >= 2:
            return True, f"Multi-season competition participant ({seasons} seasons)"
        else:
            return False, f"Competition show with insufficient participation ({episodes} eps, need 2+)"
    
    elif show_category == 'reality_series':
        # For reality series, use moderate threshold
        if episodes >= 3:
            return True, f"Reality series cast ({episodes} eps)"
        else:
            return False, f"Reality series with minimal appearance ({episodes} eps, need 3+)"
    
    else:
        # For other shows, use conservative threshold
        if episodes >= 2:
            return True, f"Unknown show type ({episodes} eps)"
        else:
            return False, f"Unknown show type with minimal appearance ({episodes} eps)"

def filter_cast_data(cast_data, show_name):
    """Filter cast data based on smart rules."""
    filtered = {}
    stats = {'included': 0, 'excluded': 0, 'reasons': {}}
    
    for imdb_id, info in cast_data.items():
        name = info.get('name', '')
        episodes = info.get('episodes')
        seasons = info.get('seasons')
        
        should_include, reason = should_include_cast_member(
            name, imdb_id, show_name, episodes, seasons
        )
        
        if should_include:
            filtered[imdb_id] = info
            stats['included'] += 1
        else:
            stats['excluded'] += 1
            stats['reasons'][reason] = stats['reasons'].get(reason, 0) + 1
    
    return filtered, stats

if __name__ == "__main__":
    # Test the filtering
    test_cases = [
        ("Celebrity Family Feud", 1, "celebrity_episodic"),
        ("Survivor", 1, "competition"), 
        ("Survivor", 3, "competition"),
        ("Big Brother", 1, "competition"),
        ("Big Brother", 5, "competition"),
        ("Celebrity Big Brother", 1, "celebrity_episodic"),
        ("Real Housewives of Atlanta", 2, "reality_series"),
        ("The Traitors US", 10, "competition")
    ]
    
    print("🧪 Testing smart filtering rules:")
    for show, eps, expected_cat in test_cases:
        category = get_show_category(show)
        should_include, reason = should_include_cast_member("Test Person", "nm123456", show, eps)
        print(f"  {show} ({eps} eps): {category} -> {'✅ INCLUDE' if should_include else '❌ EXCLUDE'} - {reason}")

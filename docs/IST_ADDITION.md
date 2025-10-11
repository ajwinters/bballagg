## In-Season Tournament (IST) Addition

### ✅ **IST Season Type Added**

**Enhancement**: Added `IST` (In-Season Tournament) to the comprehensive season types list.

### Updated Season Type Coverage
The system now includes **6 complete season types**:

1. **Regular Season** - Standard regular season games
2. **Playoffs** - Playoff games and series  
3. **Pre Season** - Preseason games (some endpoints use this format)
4. **Preseason** - Preseason games (alternative format)
5. **All Star** - All-Star Weekend events and games
6. **IST** - In-Season Tournament games (introduced 2023-24 season)

### Impact on Parameter Combinations

**Before IST Addition**:
- Season + Season Type endpoints: 29 seasons × 5 types = **145 combinations**

**After IST Addition**:  
- Season + Season Type endpoints: 29 seasons × 6 types = **174 combinations**

**Additional Coverage**: +29 combinations per endpoint with season_type parameters

### Production Impact

Endpoints like `LeagueStandingsV3`, `TeamEstimatedMetrics`, `PlayerEstimatedMetrics`, and others that use `season_type` parameters will now comprehensively collect:

- All historical In-Season Tournament data (when available)
- Complete season type coverage for comprehensive NBA data analysis
- Proper differentiation between regular season and tournament games

### Testing Validation

```
Total season types: 6
Season types included:
  1. Regular Season
  2. Playoffs  
  3. Pre Season
  4. Preseason
  5. All Star
  6. IST

✓ IST (In-Season Tournament) is included!

For endpoints with season + season_type:
  29 seasons × 6 season types = 174 total combinations
```

The distributed HPC system will now provide complete historical coverage including In-Season Tournament data for comprehensive NBA analytics.
#Notes

## Original structure of the query planner
- `SimpleDB` class uses `BasicQueryPlanner`
- `BasicQueryPlanner` uses `OptimizedProductPlan` in the function `createPlan()`
- `OptimizeProductPlan` implements a cross join
    - Each table in the query uses `TablePlan`
    - Perform the cartesian product on pairs of relation using `ProductPlan`
    - Select using `SelectPlan`
    - Project using `ProjectPlan`
    
## Original `TableScan`
- used by `TablePlan`
- `next()`: returns the next tuple, regardless of which block its in
- uses `RecordPage` to get block information
- uses `Transaction` to control buffer

## What we need to implement the 3 joins
- Read N blocks into memory 
  - option 1: modify `TableScan`
  - option 2: create a new `TableBlockScan`
- Read tuples in a block (or N blocks)
  - option 1: create a new `BlockScan`
- Write to a block
  - `RecordPage` implements `insertAfter()`
    - only sets the block as used, doesn't actually write any data
    - need to use `setInt` or `setString`
  - `BasicUpdatePlanner` has example of inserting a tuple
- Construct an in memory hash
    - option 1: get hashcode, then take LSB
    - option 2: make our own hash function

## Implementing the joins
### block nested loop
- pseudocode
```
for each block Br of r do begin
  for each block Bs of s do begin
    for each tuple tr in Br do begin
      for each tuple ts in Bs do begin
        test pair (tr, ts) to see if they satisfy the join condition
        if they do, add tr ⋅ ts to the result;
      end
    end
  end
end
```
- pseudocode for demand driven pipelining
```
variables:
  Br: M - 2 blocks of r
  Bs: one block of s
  tr: tuple in Br
  ts: tuple in Bs
  pred: join conditions

void open(r, s: tables, pred: join conditions):
  pred := join conditions in pred that contains only attributes in r and s
  tr := first of Br
  ts := before first of Bs
  
bool next():
  while Bs.next() do begin
    if test(tr, ts, pred) then return true
  end  
  // Bs used up, load next tr
  if Br.next() then begin
    // Br used up, load next Bs
    if s.next() in s then begin
      // s used up, load next Br
      if r.next() then return false // r used up, done
      Bs := first block in s
    end   
    tr := first tuple in Br
  end
  ts := first tuple in Bs
      
  return next() // keep finding until there is a match, or the end is reached  
```

### Merge-join
- assume sorted input
- pseudocode
```
pr := address of first tuple of r;
ps := address of first tuple of s;
while (ps ≠ null and pr ≠ null) do begin
  ts := tuple to which ps points;
  Ss := {ts};
  set ps to point to next tuple of s;
  done := false;
  while (not done and ps ≠ null) do begin
    ts′ := tuple to which ps points;
    if (ts′[JoinAttrs] = ts[JoinAttrs]) then begin
      Ss := Ss ∪ {ts′};
      set ps to point to next tuple of s;
    end
    else done := true;
  end
  tr := tuple to which pr points;
  while (pr ≠ null and tr[JoinAttrs] < ts[JoinAttrs]) do begin
    set pr to point to next tuple of r;
    tr := tuple to which pr points;
  end
  while (pr ≠ null and tr[JoinAttrs] = ts[JoinAttrs]) do begin
    for each ts in Ss do begin
      add ts ⋈ tr to result;
    end
    set pr to point to next tuple of r;
    tr := tuple to which pr points;
  end
end
```
- pseudocode for demand driven pipelining
```
variables:
  JA: join attribute
  
void open(r, s):

bool next():
  if not s.next() then return false
  while r[JA] < s[JA] do begin
    if not r.next() then return false
  end
  if r[JA] = s[JA] then return r.next()
  return next()
      
```

### Hash Join
- pseudocode
```
/* Partition s */
for each tuple ts in s do begin
  i := h(ts[JoinAttrs]);
  Hsi := Hsi ∪ {ts};
end
/* Partition r */
for each tuple tr in r do begin
  i := h(tr[JoinAttrs]);
  Hri := Hri ∪ {tr};
end
/* Perform join on each partition */
for i := 0 to nh do begin
  read Hsi and build an in-memory hash index on it;
  for each tuple tr in Hri do begin
    probe the hash index on Hsi to locate all tuples ts such that ts[JoinAttrs] = tr[JoinAttrs];
    for each matching tuple ts in Hsi do begin
      add tr ⋈ ts to the result;
    end
  end
end
```
- recursive partitioning

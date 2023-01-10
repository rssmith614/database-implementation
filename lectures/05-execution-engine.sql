Implement the relational algebra operators
- query tree provides the connection between relational algebra operators
- dataflow or pipeline for data processing
- how do we move data from one operator to another?
 - tuple/record (tuple-at-a-time) --> overhead for processing every record
 - batch/chunk-at-a-time
 - table (materialized execution) --> MonetDB; requires a lot of memory

GetNext(Record&) --> tuple-at-a-time; this is what we do in our project
GetNext(Record[]&) --> batch-at-a-time

Pull-based pipeline iterator
- start from the root of the tree and "pull" tuples from producer operators with GetNext() calls

WriteOut::GetNext(Record&)
  while (producer->GetNext(Record&))
    print Record in output file

RelAlg::GetNext(Record&)
  producer->GetNext(Record&)
  specific processing to this RelAlg operator
  return a result tuple to the parent

Push-based chunk execution
- start from the leaves of the tree (Scan operators) and "push" chunks to the parent operators

Database
tuples [10^20] --> can always process with tuple-at-a-time and paging

Implement GetNext(Record&) for every RelAlg operator

-- non-blocking operators
---------------
Scan::GetNext(Record&)
  Read next record from DBFile(Heap) and return it to its parent

Project::GetNext(Record&)
  producer->GetNext(Record& rec)
  rec.Project()

Select::GetNext(Record&)
	while (true) {
		bool ret = producer->GetNext(_record);
		if (false == ret) return false;

    // check the condition or evaluate the predicate
		if (true == predicate.Run(_record, constants)) return true;
	}
  
IndexScan::GetNext(Record&)
Scan + Select: push condition into the Scan operator
B+-tree index


-- blocking operators
---------------

Sum::GetNext(Record& outputRec)
  // make sure this is performed only once
  if flag is done return false

  initialize runningSum = 0
  while (producer->GetNext(Record& rec)) {
    update runningSum with returned record
    compute.Apply(rec, iRes, dRes)
    runningSum += [iRes or dRes];
  }

  build outputRec from runningSum

  set flag to done

  return true


DISTINCT
++++++++
- hash-based vs sort-based

S = {1, 2, 3, 1, 2, 3, 4, 5, 6}
R = {1, 2, 3, 4, 5, 6}

- hash-based --> hash table (map, dictionary)
map :: key --> value (data, payload)
  map :: Record --> boolean 
map::insert(key, value)
map::find(key) --> value

map = {}
for every tuple t in S do
  if map.find(t) == true
    continue
  else
    map.insert(t)
end for

iterate over the map with a stateful iterator and return the keys
map::MoveToStart
GetNext()
  if not map::AtEnd()
    outputRecord <-- map::CurrentKey()
    map::MoveToNext()
    return true
  else
    return false


- non-blocking version
map = {}
for every tuple t in S do
  if map.find(t) == true
    continue
  else
    map.insert(t)
    return to calling (parent) operator
end for

Distinct::GetNext(Record& _record)
	while (true) {
		bool ret = producer->GetNext(_record);
		if (false == ret) return false;

    _record.SetOrderMaker(order)
    
    if map.IsThere(_record) == false
      KeyInt v(0)
      map.Insert(_record, v)
      return true
    end if
	}


C++
std::map --> red-black trees : find is O(log N)
std::unordered_map --> hash table : find is O(1)


S = {1, 2, 3, 1, 2, 3, 4, 5, 6}
R = {1, 2, 3, 4, 5, 6}

S, S', and R are container data structures of Record
vector<Record> for sorting because of the need to index in data structure

sort S into S'
S' = {1, 1, 2, 2, 3, 3, 4, 5, 6}

R = {}
S'.MoveToStart()
while not S'.AtEnd()
  tmp = S'.current()
  R.append(S'.current())

  S'.MoveToNext()
  while not S'.AtEnd() AND S'.current() == tmp
    S'.MoveToNext()
  end while
end while
R = {1, 2, 3, 4, 5, 6}

Map :: Record --> KeyInt (KeyDouble)
Distinct::GetNext(Record& outputRec)
  if flag is done
    // make sure this is performed only once
    if map.AtEnd() == false
      outputRec = map.CurrentKey()
      map.Advance()
      return true
    else
      return false
    end if

  else
    // this is the first time GetNext is invoked
    // build the map
    Map map = {} // declare as class attribute
    while (producer->GetNext(Record& rec)) {
      rec.SetOrderMaker(order)
      
      if map.IsThere(rec) == false
        KeyInt v(0)
        map.Insert(rec, v)
      end if
    }

    set flag to done

    map.MoveToStart()
    if map.AtEnd() == false
      outputRec = map.CurrentKey()
      map.Advance()
      return true
    else
      return false
    end if

  end if



GROUP-BY
++++++++
map :: Record --> Aggregate (double)

map = {}
for every tuple t in S do
  if map.find(t) == true
    map[t] <-- map[t] + function.Apply(t)
    continue
  else
    map.insert(t, function.Apply(t))
end for


R : map :: Record --> double
S'.MoveToStart()
while not S'.AtEnd()
  tmp = S'.current()
  runningSum = function.Apply(S'.current())

  S'.MoveToNext()
  while not S'.AtEnd() AND S'.current() == tmp
    runningSum = runningSum + function.Apply(S'.current())
    S'.MoveToNext()
  end while

  R.insert(tmp, runningSum)
end while


Map :: Record --> KeyDouble
GroupBy::GetNext(Record& outputRec)
  if flag is done
    // make sure this is performed only once
    if map.AtEnd() == false
      // create the output record which contains both the aggregate and the grouping atts
      // first att in record is the aggregate
      Record aggRec
     	aggRec.bits = new char[2*sizeof(int) + sizeof(double)]
      ((int*)aggRec.bits)[0] = 1
      ((int*)aggRec.bits)[1] = 2*sizeof(int)
      *((double*)(aggRec.bits+2*sizeof(int))) = map.CurrentData()

      // then follow the grouping atts
      Record groupRec = map.CurrentKey()
      groupRec.Project(order.whichAtts, order.numAtts, schemaIn.GetNumAtts())

      outputRec.Append(aggRec, groupRec, 1, order.numAtts)

      map.Advance()
      return true
    else
      return false
    end if

  else
    // this is the first time GetNext is invoked
    // build the map
    Map map = {} // declare as class attribute
    while (producer->GetNext(Record& rec)) {
      rec.SetOrderMaker(order)
      
      if map.IsThere(rec) == false
        KeyDouble v = function.Apply(rec)
        map.Insert(rec, v)
      else
        KeyDouble &v = map.CurrentData()
        v = v + function.Apply(rec)
      end if

    }

    set flag to done

    map.MoveToStart()
    if map.AtEnd() == false
      // create the output record which contains both the aggregate and the grouping atts
      // first att in record is the aggregate
      Record aggRec
     	aggRec.bits = new char[2*sizeof(int) + sizeof(double)]
      ((int*)aggRec.bits)[0] = 1
      ((int*)aggRec.bits)[1] = 2*sizeof(int)
      *((double*)(aggRec.bits+2*sizeof(int))) = map.CurrentData()

      // then follow the grouping atts
      Record groupRec = map.CurrentKey()
      groupRec.Project(order.whichAtts, order.numAtts, schemaIn.GetNumAtts())

      outputRec.Append(aggRec, groupRec, 1, order.numAtts)

      map.Advance()
      return true
    else
      return false
    end if

  end if



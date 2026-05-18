# Useful Queries

# This document contains useful MongoDB queries for analyzing time series data in the `1d_stocks` collection. You can run these queries in the MongoDB shell or using a MongoDB client.

## 1. Top 5 Tickers by Number of Records
```
db.getCollection("1d_stocks").aggregate([
  {
    $group: {
      _id: "$t",
      count: {
        $sum: 1
      }
    }
  },
  {
    $sort: {
      count: -1
    }
  },
  {
    $limit: 5
  }
])
```

## 2. Get total tickers in the collection
```
db.getCollection("1d_stocks").distinct("t").length
```

## 3. Get the date range of data for a specific ticker (e.g., "TCS.NS")
```
db.getCollection("7d_stocks").aggregate([
  {
    $match: {
      t: "TCS.NS"
    }
  },
  {
    $group: {
      _id: "$t",
      minDate: {
        $min: "$ts"
      },
      maxDate: {
        $max: "$ts"
        },
        count: {
          $sum: 1
        }
    }
  }
])
```

## 4. Get bucket-level stats for a specific ticker (e.g., "TCS.NS")
```
db.getCollection("system.buckets.1d_stocks").aggregate([
  {
    $match: {
      meta: "TCS.NS"
    }
  },
  {
    $project: {
      min_datetime: "$control.min.datetime",
      max_datetime: "$control.max.datetime",
      count: "$control.count"
    }
  }
])
```

## 5. Get details across 2 datetime ranges for a specific ticker (e.g., "TCS.NS")
```
db.getCollection("7d_stocks").find({
  t: "TCS.NS",
  ts: {
    $gte: ISODate("2026-04-22T04:00:00"),
    $lte: ISODate("2026-04-23T04:59:59")
  }
})
```

## 6. Explain plan for above query
```
db.getCollection("1d_stocks").find({
  t: "TCS.NS",
  ts: {
    $gte: ISODate("2026-04-22T04:00:00"),
    $lte: ISODate("2026-04-23T04:59:59")
  }
}).sort({ ts: 1 }).explain("executionStats")
``` 

## 7. 5 min Agg for Open, close, high, low for a ticker (e.g., "TCS.NS")
```
db.getCollection("7d_stocks").aggregate([
  {
    $match: {
      t: "TCS.NS",
        ts: {
            $gte: ISODate("2026-05-11T04:00:00"),
            $lte: ISODate("2026-05-15T04:59:59")
        }
    }
  },
  {
    $group: {
      _id: {
        $dateTrunc: {
          date: "$ts",
          unit: "minute",
          binSize: 15
        }
      },
      o: {
        $first: "$o"
      },
      c: {  
        $last: "$c"
      },
      h: {
        $max: "$h"
      },
      l: {
        $min: "$l"
      }
    }
  },
  {
    $sort: {
      "_id": 1
    }
  }
])
```

# Queries for Bucket Collection
## 1. Count Number of Buckets for a Ticker (e.g., "TCS.NS")
```
db.getCollection("system.buckets.1d_stocks").countDocuments({ meta: "TCS.NS" })
``` 


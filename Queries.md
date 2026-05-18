# Useful Queries

# This document contains useful MongoDB queries for analyzing time series data in the `ohlc_min` collection. You can run these queries in the MongoDB shell or using a MongoDB client.

## 1. Top 5 Tickers by Number of Records
```
db.ohlc_min.aggregate([
  {
    $group: {
      _id: "$ticker",
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
db.ohlc_min.distinct("ticker").length
```

## 3. Get the date range of data for a specific ticker (e.g., "TCS.NS")
```
db.ohlc_min.aggregate([
  {
    $match: {
      ticker: "TCS.NS"
    }
  },
  {
    $group: {
      _id: "$ticker",
      minDate: {
        $min: "$datetime"
      },
      maxDate: {
        $max: "$datetime"
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
db.getCollection("system.buckets.ohlc_min").aggregate([
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
db.ohlc_min.find({
  ticker: "TCS.NS",
  datetime: {
    $gte: ISODate("2026-04-22T04:00:00"),
    $lte: ISODate("2026-04-23T04:59:59")
  }
})
```

## 6. Explain plan for above query
```
db.ohlc_min.find({
  ticker: "TCS.NS",
  datetime: {
    $gte: ISODate("2026-04-22T04:00:00"),
    $lte: ISODate("2026-04-23T04:59:59")
  }
}).sort({ datetime: 1 }).explain("executionStats")
``` 

## 7. 5 min Agg for Open, close, high, low for a ticker (e.g., "TCS.NS")
```
db.ohlc_min.aggregate([
  {
    $match: {
      ticker: "TCS.NS",
        datetime: {
            $gte: ISODate("2026-04-22T04:00:00"),
            $lte: ISODate("2026-04-23T04:59:59")
        }
    }
  },
  {
    $group: {
      _id: {
        $dateTrunc: {
          date: "$datetime",
          unit: "minute",
          binSize: 5
        }
      },
      open: {
        $first: "$open"
      },
      close: {  
        $last: "$close"
      },
      high: {
        $max: "$high"
      },
      low: {
        $min: "$low"
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
db.getCollection("system.buckets.ohlc_min").countDocuments({ meta: "TCS.NS" })
``` 


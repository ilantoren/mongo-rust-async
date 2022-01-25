use tokio::time::Duration;
use core::fmt;
use std::fmt::{Display, Formatter};
use mongodb::{bson::doc, bson::Bson, bson::Document, options::ClientOptions, options::FindOptions, Client, Collection, Cursor};
use futures::{TryStreamExt, StreamExt};
use serde::{Serialize, Deserialize};
use std::borrow::Borrow;
use mongodb::options::AggregateOptions;
use mongodb::error::Error;

fn main() {
    process()
}

const  DB: &str = "off";
const SOURCE: &str = "products";
const TARGET: &str = "usda";


#[derive(Serialize,Deserialize, Debug)]
pub struct Usda {
    id: String,
    product_name: String,
    url: String
}
impl fmt::Display for Usda {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {} product: {} url: {} ", self.id, self.product_name, self.url)
    }
}

struct Err {}
impl From<mongodb::error::Error> for Err {
    fn from(_error: mongodb::error::Error) -> Self {
        Err {}
    }
}
impl Display for Err {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}
type Result<T> = std::result::Result<T, Err>;

#[tokio::main]
pub async fn process() {

    let conn = get_connection().await;
    match conn {
        Ok(connection) => {
            println!("connection is valid");
            create_usda_set(&connection).await;
            test_usda( &connection ).await;
            aggregate_test( &connection ).await;
        }
        Err(e) => eprintln!("There is an error: {}",e),
    }
}
/*
    Take a document and extract the fields to create a Usda object, then
    insert the document (one at a time) into the new Usda collection
 */
async fn create_usda_set<'a>(client: &Client) {
    let read_database = client.database(DB).collection::<Document>(SOURCE);
    // as opposed to javascript/python/kotlin the projection is part of the find_options. The batch size is something you want to
    // have with the async driver and arguably also with the sync since long queries can lose the cursor.
    let find_options = FindOptions::builder().projection( doc!{"id":1, "product_name":1, "sources.url":1}).batch_size(1000).build();
     // filter to look for records with a creator : usda and with a sources.url field
    let filter = doc!{ "$and": [ { "creator": {"$regex": r"\busda\b", "$options": "i" }}, {"sources.url": {"$exists": 1}}]};
    let cursor = read_database.find(filter, find_options).await;
    match cursor {
        Ok( mut cur) => {
            while let Some(doc) = cur.next().await {
                match doc {
                    Ok( d ) => {
                       persist ( d.borrow(), &client).await;
                    }
                    _ => eprintln!("no document")
                }
            }
        },
        Err(e) => eprintln!("Error reading {}", Err::from(e))
    }
}
/*
    Take a document and extract the fields to create a Usda object, then
    insert the document (one at a time) into the new Usda collection
 */
async fn persist(d: &Document, client: &Client) {
    let collection = client.database(DB).collection::<Usda>(TARGET);
    let id = d.get_str("_id").unwrap().to_string();
    let product_name = d.get_str("product_name").unwrap_or_default().to_string();
    let r_arr = d.get_array("sources").unwrap();
    let first_element = r_arr[0].as_document().unwrap();
    let url =  first_element.get_str("url").unwrap_or_default().to_string();
    let record = Usda { id, product_name,  url};
    let handle = tokio::spawn( async move {
        let res = collection.insert_one(record, None).await;
        match res {
            Ok(uid) => println!("{}", uid.inserted_id),
            Err(e) => eprintln!("{}", Err::from(e))
        }
    });
    let _r =  handle.await;

}

// Parse your connection string into an options struct
async fn get_connection<'a>() -> Result<mongodb::Client> {
    let connect_string = "mongodb://localhost:27017";

    let mut client_options = ClientOptions::parse(&connect_string).await?;
    // Manually set an option
    let duration: Duration = Duration::new(60, 0);
    client_options.app_name = Some("Rust Demo".to_string());
    client_options.connect_timeout = Some(duration);
    // Get a handle to the cluster
    let client = Client::with_options(client_options)?;
    // Ping the server to see if you can connect to the cluster
    client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)
        .await?;

    Ok(client)
}

async fn create_test_set(client: &Client ) {
    let src = client.database("off").collection::<Document>("products");
    let targ = client.database("off").collection::<Document>("test_set");
    let find_options = FindOptions::builder().limit( 10000 ).build();
    let cursor = src.find(doc! {}, find_options).await;
    match cursor {
        Ok(mut cur) => loop {
            let document = cur.try_next().await;
            {
                match document {
                    Ok(d) => {
                        match d {
                            Some(d) => match targ.insert_one(d, None).await {
                                Ok(report) => { println!("inserted {}", report.inserted_id) }
                                Err(_) => {}
                            },
                            _ => {}
                        }
                    }
                    Err(_) => {}
                }
            }
        },
        Err(_) => eprintln!("error")
    }
}

/*
    Using only aggregation.  Read the source collection and create Usda objects
    Note that the collection::<Usda> doesn't work here (please fiddle the code and
    try it yourself
 */
async fn aggregate_test<'a>(client: &Client) {
    let pipeline = [
        doc! {"$match": {
           "$and": [
               {
                   "creator": {
                   "$regex": "usda",
                   "$options": "i"
               }
               }, {
                   "sources.url": {
                       "$exists": 1
                   }
               }
           ]
       }
       },
        doc! {
           "$project": {
               "_id": 1,
               "url": "$sources.url",
               "product_name": 1
           }
       },
        doc! {
           "$unwind": {
               "path": "$url",
               "includeArrayIndex": "ind",
               "preserveNullAndEmptyArrays": false
           }
       },
        doc! {
           "$match": {
               "ind": 0
           }
       },
        /*doc! {
           "$limit": 4000
       },*/];
    let src = client.database("off").collection::<Document>("products");
    let mut options = AggregateOptions::default();
    options.batch_size = Some(100);
   let wait  = Duration::new(360, 0);
    options.max_time = Some(wait) ;
    options.allow_disk_use = Some(true);
    let cursor = src.aggregate(pipeline, options).await;
    match cursor {
        Ok( mut cur) =>
            while let Some(document) = cur.try_next().await.unwrap_or_default() {
                println!("{}", document)
        },
        _ => println!("No results returned")
    }
}


/*
   Read from the Usda collection directly into Usda objects
 */
async fn test_usda(client: &Client) {
    let collection = client.database("off").collection::<Usda>("usda");
    let options = FindOptions::builder().batch_size(500).limit(62000).build();
    let mut counter: i32  = 0;
    match collection.find(doc! {}, options).await {
        Ok(mut cursor) =>
            while let Some(record) = cursor.next().await {
                counter += 1;
                match record {
                    Ok(r) => println!("{} {}", counter, r),
                    _ => eprintln!("error")
                }
            }
        _ => {}
    }
    return
}
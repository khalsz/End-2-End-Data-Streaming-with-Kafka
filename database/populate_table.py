from configure.posgres_config import db_config
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base

# Initialzing table classes
Base = declarative_base()

# Defining walway table class
class WalkWayTable(Base):
    __tablename__ = 'walkway'

    osm_id = Column(Integer, primary_key=True)
    osm_type = Column(String)
    layer = Column(String)
    surface = Column(String)
    highway = Column(String)
    tunnel = Column(String)
    bridge = Column(String)
    longitude = Column(Float)
    latitude = Column(Float)
    geom_type = Column(String)
    location = Column(String)

    def __repr__(self):
        return f"WalkWayTable(osm_id={self.osm_id}, osm_type='{self.osm_type}', layer={self.layer}, surface='{self.surface}', highway='{self.highway}', tunnel='{self.tunnel}', bridge='{self.bridge}', longitude={self.longitude}, latitude={self.latitude}, geom_type='{self.geom_type}', location='{self.location}')"


def create_tab(): 
  
  try: 
    engine = create_engine(f"postgresql+psycopg2://{db_config['username']}:{db_config['password']}@localhost:5432/transport")
    
    # create table in database
    print("Creating table...")   
    Base.metadata.create_all(engine)
    print("Table created successfully")
    return engine

  except Exception as e:
    print(f"Error creating table: {e}")

# populate postgres table walkway
def populate_postgres_table(dataframe, batch_id): 
  try:
      
      print("Populating table...")
      # print("Batch_id: " + str(batch_id))
      
      # dataframe.write.format("csv").mode("append").save("data/outdata.csv")
      # query_statement = text('''INSERT INTO walkway (osm_id, osm_type, layer, 
      #                        surface, highway, tunnel, bridge, longitude, 
      #                        latitude, geom_type, location)
      #                            VALUES (:osm_id, :osm_type, :layer, :surface, 
      #                            :highway, :tunnel, :bridge, :longitude, 
      #                             :latitude, :geom_type, :location)'''
      #                             )
      # with engine.connect() as conn: 
      #   conn.execute(query_statement, dataframe)
      # conn.commit()
      # print("Table populated successfully")
      # print(dataframe)
      url = "jdbc:postgresql://localhost:5432/transport" 
      
      # properties = {
      #     "driver": "org.postgresql.Driver",
      #     "user": db_config["username"],
      #     "password": db_config["password"]
      # }
      # dataframe.write.jdbc(url=url, table="walkway", mode="append", properties=properties)
      dataframe.write.mode("append").format("jdbc").option("url", url) \
                          .option("driver", "org.postgresql.Driver") \
                          .option("dbtable", "walkway") \
                          .option("user", db_config["username"]) \
                          .option("password", db_config["password"]).save()
      print("Table populated successfully")
  except Exception as e:
      print(f"Error populating table: {e}")







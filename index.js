const express = require('express');
const {BigQuery} = require('@google-cloud/bigquery');
const cors = require('cors');
const bigquery = new BigQuery();
const app = express();
	
const iot = require('@google-cloud/iot');
const iotClient = new iot.v1.DeviceManagerClient({
  // optional auth parameters.
});

const corsOptions = { origin:"*", optionsSucessStatus:200}
const port = process.env.PORT || 8080;

app.use(cors(corsOptions));
app.use(express.json());
app.listen(port, () => {
  console.log(`helloworld: listening on port ${port}`);
});


app.get('/devices/:id/last-status', async function(req, res) {
  let data = await queryLastStatus(req.params.id);
  res.status(200).send(data);
});

app.get('/devices/:id', async function(req, res) {
  let data = await queryDevice(req.params.id);
  res.status(200).send(data);
});


app.get('/devices', async function(req, res) {
  let data = await query();
  res.status(200).send(data);
});

app.get('/devices/:id/history', async function(req, res) {
  let data = await queryHistory(req.params.id);
  res.status(200).send(data);
});

app.put('/devices/:deviceId/led-status', async function(req, res) {
  let data = await queryDevice(req.params.deviceId);
  if(data.length == 0) {
    res.status(400).send('No existe');
  }

  const formattedName = iotClient.devicePath(
    'daiot-smart-home',
    'us-central1',
    'daiot-registry',
    data[0].dev_id_gcp
  );
  const myJSON = JSON.stringify({led:req.body.state});
  const binaryData = Buffer.from(myJSON);

  const request = {
    name: formattedName,
    binaryData: binaryData,
  };

  const [response] = await iotClient.sendCommandToDevice(request);
  console.log('Sent command: ', response);

  res.status(200).send(binaryData);
});



async function queryLastStatus(devId) {
  let data = [];

  const sqlQuery = `SELECT timestamp, temperatura, humedad
        FROM \`daiot-smart-home.devices.sensores_tph_origen\`
        WHERE dev_id =  @dev_id
        ORDER BY timeStamp DESC
        LIMIT 1`;
      
      
  const options = {
      query: sqlQuery,
      params: {
        dev_id:parseInt(devId)
      },
      types: {
        dev_id:'INT64'
      },
      location: 'us-central1',
    };

      // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

    // Wait for the query to finish
  const [rows] = await job.getQueryResults();
  rows.forEach(row => data.push(row));
      // Print the results
      console.log('rows' + rows);
      console.log('data in query' + data);
  return data;

  }


  async function queryDevice(devId) {
    let data = [];
  
    const sqlQuery = `select t3.*, humedad, temperatura, led_1, t4.timestamp
    from daiot-smart-home.devices.sensores_tph_desc_ubicacion t3
    left join  (
      select t1.* from daiot-smart-home.devices.sensores_tph_origen as t1
      inner join (
          select max(timestamp) as timestamp,dev_id
          from daiot-smart-home.devices.sensores_tph_origen
          where dev_id = @dev_id
          group by  dev_id) as t2
           
    on t1.timestamp = t2.timestamp) as t4
    on t3.dev_id = t4.dev_id
    where t4.dev_id = @dev_id`;
        
        
    const options = {
        query: sqlQuery,
        params: {
          dev_id:parseInt(devId)
        },
        types: {
          dev_id:'INT64'
        },
        location: 'us-central1',
      };
  
        // Run the query as a job
    const [job] = await bigquery.createQueryJob(options);
    console.log(`Job ${job.id} started.`);
  
      // Wait for the query to finish
    const [rows] = await job.getQueryResults();
    rows.forEach(row => {
      data.push({
        dev_id:row.dev_id,
        ubicacion:row.ubicacion,
        propietario:row.propietario,
        descripcion:row.descripcion,
        temp_offset:row.temp_offset,
        humedad:row.humedad,
        temperatura:row.temperatura,
        timestamp:row.timestamp,
        dev_id_gcp:row.dev_id_gcp,
        led_1:row.led_1
      });
    } );
        // Print the results
        console.log('rows' + rows);
        console.log('data in query' + data);
    return data;
  
    }
 
async function query() {
  let data = [];

  const sqlQuery = `SELECT *
        FROM \`daiot-smart-home.devices.sensores_tph_desc_ubicacion\``;
      
      
  const options = {
      query: sqlQuery,
      // Location must match that of the dataset(s) referenced in the query.
        location: 'us-central1',
    };

      // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

    // Wait for the query to finish
  const [rows] = await job.getQueryResults();
  rows.forEach(row => data.push(row));
      // Print the results
      console.log('rows' + rows);
      console.log('data in query' + data);
  return data;
      
}

async function queryHistory(devId) {
  let data = [];

  const sqlQuery = `SELECT *
        FROM \`daiot-smart-home.devices.sensores_tph_30min_interval\`
        WHERE dev_id =  @dev_id`;
      
      
  const options = {
      query: sqlQuery,
      params: {
        dev_id:parseInt(devId)
      },
      types: {
        dev_id:'INT64'
      },
      location: 'us-central1',
    };

      // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

    // Wait for the query to finish
  const [rows] = await job.getQueryResults();
  rows.forEach(row => data.push(row));
      // Print the results
      console.log('rows' + rows);
      console.log('data in query' + data);
  return data;

  }
 
const pg = require('pg');

const pool = new pg.Pool({
  user: process.env.RDS_USER,
  password: process.env.RDS_PASSWORD,
  host: process.env.RDS_HOST,
  database: process.env.RDS_DB,
  port: process.env.RDS_PORT,
  ssl: { rejectUnauthorized: false } 
});

exports.handler = async (event, context) => {
  try {
    if (event.path === '/health' && event.httpMethod === 'GET') {
      return {
        statusCode: 200,
        body: JSON.stringify({ message: "Healthy" })
      };
    }

    let body = event.body && typeof event.body === 'string' ? JSON.parse(event.body) : event.body;

    // Login
    if (event.path === '/login' && event.httpMethod === 'POST') {
      const loginQuery = 'SELECT * FROM users WHERE name = $1 AND password = $2';
      const loginResponse = await pool.query(loginQuery, [body.name, body.password]);
      if (loginResponse.rows.length > 0 && loginResponse.rows[0].name === body.name && loginResponse.rows[0].password === body.password) {
        return {
          statusCode: 200,
          body: JSON.stringify({ message: 'Login Successful' })
        };
      } else {
        return {
          statusCode: 401,
          body: JSON.stringify({ message: 'Invalid Credentials' })
        };
      }
    }

    // Get countries
    if (event.path === '/countries' && event.httpMethod === 'GET') {
      let countriesQuery = 'SELECT * FROM countries';
      let cQueryResults = await pool.query(countriesQuery);
      if (cQueryResults.rows.length > 0) {
        return {
          statusCode: 200,
          body: JSON.stringify(cQueryResults.rows)
        };
      } else {
        return {
          statusCode: 400,
          body: JSON.stringify({ message: "No countries available" })
        };
      }
    }

    // Get cities
    if (event.path === '/cities' && event.httpMethod === 'GET') {
      if (event.queryStringParameters && event.queryStringParameters.countryId) {
        let citiesQuery = 'SELECT * FROM cities WHERE country_id = $1';
        let citiesQueryResults = await pool.query(citiesQuery, [event.queryStringParameters.countryId]);
        if (citiesQueryResults.rows.length > 0) {
          return {
            statusCode: 200,
            body: JSON.stringify(citiesQueryResults.rows)
          };
        } else {
          return {
            statusCode: 400,
            body: JSON.stringify({ message: "No cities available" })
          };
        }
      } else {
        return {
          statusCode: 400,
          body: JSON.stringify({ message: "No cities available" })
        };
      }
    }

    // Get categories
    if (event.path === '/categories' && event.httpMethod === 'GET') {
      let categoriesQuery = 'SELECT * FROM categories';
      let categoriesQueryResult = await pool.query(categoriesQuery);
      if (categoriesQueryResult.rows.length > 0) {
        return {
          statusCode: 200,
          body: JSON.stringify(categoriesQueryResult.rows)
        };
      } else {
        return {
          statusCode: 400,
          body: JSON.stringify({ message: "No categories available" })
        };
      }
    }

    // Fetch listings
    if (event.path === '/listings' && event.httpMethod === 'GET') {
      if (event.queryStringParameters && event.queryStringParameters.cityId && event.queryStringParameters.categoryId) {
        let listingsQuery = 'SELECT * FROM listings WHERE city_id = $1 AND category_id = $2';
        let listingsQueryResults = await pool.query(listingsQuery, [event.queryStringParameters.cityId, event.queryStringParameters.categoryId]);
        if (listingsQueryResults.rows.length > 0) {
          return {
            statusCode: 200,
            body: JSON.stringify(listingsQueryResults.rows)
          };
        } else {
          return {
            statusCode: 200,
            body: JSON.stringify({ message: "No listings available." })
          };
        }
      } else {
        return {
          statusCode: 400,
          body: JSON.stringify({ message: "No city or category available." })
        };
      }
    }

    // Get appointments
    if (event.path.match(/\/listings\/[^\/]+\/appointments/) && event.httpMethod === 'GET') {
      if (!event.pathParameters || !event.pathParameters.listingId) {
        return {
          statusCode: 400,
          body: JSON.stringify({ message: "No appointments available for this listing." })
        };
      }
      let listingId = event.pathParameters.listingId;
      let listingQuery = 'SELECT city_id, category_id FROM listings WHERE id = $1';
      let listingQueryResults = await pool.query(listingQuery, [listingId]);
      if (listingQueryResults.rows.length > 0) {
        let cityQuery = "SELECT LOWER(REPLACE(city_name, ' ', '_')) AS city_name FROM cities WHERE id = $1";
        let cityQueryResults = await pool.query(cityQuery, [listingQueryResults.rows[0].city_id]);
        if (cityQueryResults.rows.length > 0) {
          let cityName = cityQueryResults.rows[0].city_name;
          let appointmentTable = `available_appointments_${cityName}`;
          let availableAppointments = `SELECT DISTINCT ON (agent_id, date_time) id, ${appointmentTable}.agent_id, date_time, duration, status 
                            FROM ${appointmentTable} 
                            JOIN agent_categories ON ${appointmentTable}.agent_id = agent_categories.agent_id 
                            WHERE ${appointmentTable}.status = 'Available' 
                            AND agent_categories.category_id = $1 
                            ORDER BY ${appointmentTable}.agent_id, date_time`;
          let availableAppointmentsResults = await pool.query(availableAppointments, [listingQueryResults.rows[0].category_id]);
          if (availableAppointmentsResults.rows.length > 0) {
            return {
              statusCode: 200,
              body: JSON.stringify(availableAppointmentsResults.rows)
            };
          } else {
            return {
              statusCode: 400,
              body: JSON.stringify({ message: "No available appointments at the moment." })
            };
          }
        } else {
          console.log("No city associated with listing was found.");
          return {
            statusCode: 500,
            body: JSON.stringify({ message: "Internal server error." })
          };
        }
      } else {
        console.log("No listing was found.");
        return {
          statusCode: 500,
          body: JSON.stringify({ message: "Internal server error." })
        };
      }
    }

    // Submit appointments
    if (event.path.match(/\/listings\/[^\/]+\/appointments/) && event.httpMethod === 'POST') {
      if (event.body && event.body.listingId && event.body.slotId &&
          event.body.clientPhone && event.body.contactMethod && event.body.clientName &&
          event.body.appointmentDate) {
        let getCity = 'SELECT city_id FROM listings WHERE id = $1';
        let getCityResults = await pool.query(getCity, [event.body.listingId]);
        if (getCityResults.rows.length === 0) {
          console.log("No listing was found.");
          return {
            statusCode: 500,
            body: JSON.stringify({ message: "Internal server error." })
          };
        }

        let cityName = "SELECT LOWER(REPLACE(city_name, ' ', '_')) AS city_name FROM cities WHERE id = $1";
        let cityNameResults = await pool.query(cityName, [getCityResults.rows[0].city_id]);
        if (cityNameResults.rows.length === 0) {
          console.log("No city associated with listing was found.");
          return {
            statusCode: 500,
            body: JSON.stringify({ message: "Internal server error." })
          };
        }

        let cityAppointmentTable = `available_appointments_${cityNameResults.rows[0].city_name}`;
        let isAvailable = `SELECT * FROM ${cityAppointmentTable} WHERE status = 'Available' AND date_time = $1 AND id = $2`;
        let isAvailableResults = await pool.query(isAvailable, [event.body.appointmentDate, event.body.slotId]);

        if (isAvailableResults.rows.length > 0) {
          let bookAppt = `UPDATE ${cityAppointmentTable} SET status = 'Booked' WHERE id = $1 AND status = 'Available'`;
          let bookApptResults = await pool.query(bookAppt, [event.body.slotId]);

          let addressQuery = 'SELECT address FROM listings WHERE id = $1';
          let addressResults = await pool.query(addressQuery, [event.body.listingId]);

          let scheduleAppt = `INSERT INTO appointments (listing_id, client_phone, contact_method, agent_id, client_name, appointment_date, location, status, notes) 
                              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`;
          let scheduleApptResults = await pool.query(scheduleAppt, [
            event.body.listingId, 
            event.body.clientPhone, 
            event.body.contactMethod, 
            isAvailableResults.rows[0].agent_id, 
            event.body.clientName, 
            event.body.appointmentDate, 
            addressResults.rows[0].address, 
            'Scheduled', 
            event.body.notes || null
          ]);

          if (bookApptResults.rowCount === 1 && scheduleApptResults.rowCount === 1) {
            return {
              statusCode: 200,
              body: JSON.stringify({ message: "Appointment booked." })
            };
          } else {
            return {
              statusCode: 500,
              body: JSON.stringify({ message: "We couldn't process your request, please try again." })
            };
          }
        } else {
          return {
            statusCode: 400,
            body: JSON.stringify({ message: "We're sorry, appointment no longer available, please select another time slot." })
          };
        }
      } else {
        return {
          statusCode: 400,
          body: JSON.stringify({ message: "There are missing required fields. Please submit your appointment again." })
        };
      }
    }

  } catch (error) {
    console.error('Error details:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ message: 'Server Error Response' })
    };
  }
};


// ============================================================
// API HELPER
// ============================================================

async function getData(url) {

    const response = await fetch(url);

    if (!response.ok) {

        throw new Error(
            `API request failed: ${response.status}`
        );

    }

    return await response.json();
}


// ============================================================
// FORMATTERS
// ============================================================

function formatNumber(value) {

    return Number(value).toLocaleString(
        "en-US"
    );
}


function formatCurrency(value) {

    return "$" +
        Number(value).toLocaleString(
            "en-US",
            {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            }
        );
}


function formatRating(value) {

    return Number(value).toFixed(2);
}


// ============================================================
// KPI DASHBOARD
// ============================================================

async function loadKPIs() {

    const containerIds = [
        "total-rides",
        "total-revenue",
        "average-fare",
        "average-rating",
        "total-tips",
        "average-distance"
    ];

    try {

        const data =
            await getData("/api/kpis");


        // Total rides

        document.getElementById(
            "total-rides"
        ).textContent =
            formatNumber(data.total_rides);


        // Revenue

        document.getElementById(
            "total-revenue"
        ).textContent =
            formatCurrency(data.total_revenue);


        // Average fare

        document.getElementById(
            "average-fare"
        ).textContent =
            formatCurrency(data.average_fare);


        // Rating

        document.getElementById(
            "average-rating"
        ).textContent =
            formatRating(data.average_rating);


        // Tips

        document.getElementById(
            "total-tips"
        ).textContent =
            formatCurrency(data.total_tips);


        // Distance

        document.getElementById(
            "average-distance"
        ).textContent =
            Number(
                data.average_distance
            ).toFixed(2) + " mi";


    } catch (error) {

        console.error(
            "KPI error:",
            error
        );

        containerIds.forEach(id => {

            document.getElementById(id)
                .textContent = "Error";

        });

    }
}


// ============================================================
// VEHICLE TABLE
// ============================================================

async function loadVehicles() {

    const container =
        document.getElementById(
            "vehicle-data"
        );

    try {

        const data =
            await getData(
                "/api/vehicles"
            );


        let html = `

            <table>

                <thead>

                    <tr>

                        <th>
                            Vehicle Type
                        </th>

                        <th>
                            Rides
                        </th>

                        <th>
                            Revenue
                        </th>

                    </tr>

                </thead>

                <tbody>

        `;


        data.forEach(row => {

            html += `

                <tr>

                    <td>
                        ${row.vehicle_type ?? "Unknown"}
                    </td>

                    <td>
                        ${formatNumber(
                            row.total_rides
                        )}
                    </td>

                    <td>
                        ${formatCurrency(
                            row.revenue
                        )}
                    </td>

                </tr>

            `;

        });


        html += `

                </tbody>

            </table>

        `;


        container.innerHTML = html;


    } catch (error) {

        console.error(
            "Vehicle error:",
            error
        );

        container.innerHTML =
            `<div class="error">
                Unable to load vehicle data.
            </div>`;
    }
}


// ============================================================
// PAYMENT TABLE
// ============================================================

async function loadPayments() {

    const container =
        document.getElementById(
            "payment-data"
        );

    try {

        const data =
            await getData(
                "/api/payments"
            );


        let html = `

            <table>

                <thead>

                    <tr>

                        <th>
                            Payment Method
                        </th>

                        <th>
                            Rides
                        </th>

                        <th>
                            Revenue
                        </th>

                    </tr>

                </thead>

                <tbody>

        `;


        data.forEach(row => {

            html += `

                <tr>

                    <td>
                        ${row.payment_method ?? "Unknown"}
                    </td>

                    <td>
                        ${formatNumber(
                            row.total_rides
                        )}
                    </td>

                    <td>
                        ${formatCurrency(
                            row.revenue
                        )}
                    </td>

                </tr>

            `;

        });


        html += `

                </tbody>

            </table>

        `;


        container.innerHTML = html;


    } catch (error) {

        console.error(
            "Payment error:",
            error
        );

        container.innerHTML =
            `<div class="error">
                Unable to load payment data.
            </div>`;
    }
}


// ============================================================
// CITY TABLE
// ============================================================

async function loadCities() {

    const container =
        document.getElementById(
            "city-data"
        );

    try {

        const data =
            await getData(
                "/api/cities"
            );


        let html = `

            <table>

                <thead>

                    <tr>

                        <th>
                            City
                        </th>

                        <th>
                            Rides
                        </th>

                        <th>
                            Revenue
                        </th>

                    </tr>

                </thead>

                <tbody>

        `;


        data.forEach(row => {

            html += `

                <tr>

                    <td>
                        ${row.pickup_city ?? "Unknown"}
                    </td>

                    <td>
                        ${formatNumber(
                            row.total_rides
                        )}
                    </td>

                    <td>
                        ${formatCurrency(
                            row.revenue
                        )}
                    </td>

                </tr>

            `;

        });


        html += `

                </tbody>

            </table>

        `;


        container.innerHTML = html;


    } catch (error) {

        console.error(
            "City error:",
            error
        );

        container.innerHTML =
            `<div class="error">
                Unable to load city data.
            </div>`;
    }
}


// ============================================================
// DRIVER TABLE
// ============================================================

async function loadDrivers() {

    const container =
        document.getElementById(
            "driver-data"
        );

    try {

        const data =
            await getData(
                "/api/drivers"
            );


        let html = `

            <table>

                <thead>

                    <tr>

                        <th>
                            Driver
                        </th>

                        <th>
                            Rides
                        </th>

                        <th>
                            Revenue
                        </th>

                        <th>
                            Rating
                        </th>

                    </tr>

                </thead>

                <tbody>

        `;


        data.forEach(row => {

            html += `

                <tr>

                    <td>
                        ${row.driver_name ?? "Unknown"}
                    </td>

                    <td>
                        ${formatNumber(
                            row.total_rides
                        )}
                    </td>

                    <td>
                        ${formatCurrency(
                            row.revenue
                        )}
                    </td>

                    <td>
                        ${formatRating(
                            row.average_rating
                        )}
                    </td>

                </tr>

            `;

        });


        html += `

                </tbody>

            </table>

        `;


        container.innerHTML = html;


    } catch (error) {

        console.error(
            "Driver error:",
            error
        );

        container.innerHTML =
            `<div class="error">
                Unable to load driver data.
            </div>`;
    }
}


// ============================================================
// LOAD COMPLETE DASHBOARD
// ============================================================

async function loadDashboard() {

    await Promise.all([

        loadKPIs(),

        loadVehicles(),

        loadPayments(),

        loadCities(),

        loadDrivers()

    ]);

}


// ============================================================
// START DASHBOARD
// ============================================================

loadDashboard();
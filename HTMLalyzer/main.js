// expect to be called from python
async function chartsOutputGraph(id, x, ys, colors) {
	id = JSON.parse(id);
	x = JSON.parse(x);
	ys = JSON.parse(ys);

	// resize canvas before drawing to it
	let canvas = document.getElementById("chart" + id);
	canvas.style.width='100%';
	canvas.style.height='100%';
	canvas.width  = canvas.offsetWidth;
	canvas.height = canvas.offsetHeight;

	let datasets = [];
	let scales = {};
	// add ys to dataset
	for (const [key, value] of Object.entries(ys)) {
		// colors = ys["colors"]
		// y_vals = ys["vals"]
		datasets.push({
			data: value["vals"],
			label: key + " (" + value["units"] + ")",
			borderColor: value["color"],
			// yAxisID: key,
		});
		// scales[key] = {
		// 	type: "linear",
		// 	display: true,
		// 	grid: {
		// 		drawOnChartArea: false, // only want the grid lines for one axis to show up
		// 	},

		// }
	}

	// create chart pointing to the file's chart canvas
	new Chart(document.getElementById("chart"+id), {
		type: 'line',
		data: {
			labels: x,
			datasets: datasets
		},

		options: {
			responsive: true,
			plugins: {
				legend: {
					position: 'top',
				},
				title: {
					display: true,
					text: 'Chart.js Line Chart'
				}
			}
		},

	});



}

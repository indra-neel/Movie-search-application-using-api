document.getElementById('movieForm').addEventListener('submit', function(e) {
    e.preventDefault();

    const movieName = document.getElementById('movieName').value;
    const apiKey = '9a755658';

    fetch(`https://www.omdbapi.com/?s=${encodeURIComponent(movieName)}&apikey=${apiKey}`)
        .then(response => response.json())
        .then(data => {
            const movieResults = document.getElementById('movieResults');
            movieResults.innerHTML = '';

            if (data.Response === 'True') {
                data.Search.forEach(movie => {
                    const movieItem = document.createElement('div');
                    movieItem.classList.add('movie-item');
                    movieItem.innerHTML = `
                        <img src="${movie.Poster !== "N/A" ? movie.Poster : 'placeholder.jpg'}" alt="${movie.Title}">
                        <h2>${movie.Title}</h2>
                        <p>Year: ${movie.Year}</p>
                    `;
                    movieResults.appendChild(movieItem);
                });
            } else {
                movieResults.innerHTML = `<p>${data.Error}</p>`;
            }
        })
        .catch(error => {
            console.log('Error:', error);
            document.getElementById('movieResults').innerHTML = `<p>An error occurred. Please try again.</p>`;
        });
});

# How to Sync a New MongoDB Collection to Elasticsearch

1. **Create a Mapping File**

   - Create a mapping file in the `elasticsearch-template` directory similar to the existing files.
   - Copy the entire file and paste it into the Elasticsearch console to create the index.

2. **Create a Transform Script**

   - Create a transform script in the `scripts` directory to filter or parse data from MongoDB before syncing to Elasticsearch. Use the existing scripts as a reference.

3. **Create a Configuration File**

   - Create a `config.toml` file based on the `config.sample.toml` template.

4. **Add Environment Variables**

   - Add a `.env` file based on the `.env.example` file.

5. **Build and Run the Docker Container**

   - Run the command `docker-compose up -d --build`.

6. **Check Logs**

   - Check the logs in the `logs` directory to ensure everything is working correctly.

7. **Push Code to Bitbucket**
   - Before pushing the code to Bitbucket, add the configuration from `config.toml` to `config.sample.toml` to avoid it being ignored by .gitignore .

Noted:

- To read the monstache logs file, set chmod folder logs: `sudo chmod 755 -R monstache/logs`

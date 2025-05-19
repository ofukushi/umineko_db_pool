
これは保存してある json ファイルを読み込んでパースしてテーブルに書き込むコードです
当初は fins_all テーブルだけを新たにr作成してリプレイスするように考えていましたが、
このままその他のdependent scripts をコールしてそれらのテーブルをアップデートするのもありなのかと思いました


🛠️ Build & Run
This command tells Docker to:
    Build an image from the Dockerfile in the current directory (.).
    Tag it as jquants-pipeline — this becomes the image name you’ll use later to run it.
    
# Build the Docker image
docker build -t jquants-pipeline .

# Run the container with .env mounted
docker run --rm \
  -v $(pwd)/.env.umineko_db_pool:/app/.env.umineko_db_pool \
  jquants-pipeline


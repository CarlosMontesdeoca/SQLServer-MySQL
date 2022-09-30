<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Conexion SQL</title>
</head>
<body>
    <?php 
        try {
            $conn = new PDO ("sqlsrv:Server=192.168.9.221,1433;Database=SisMetPrec",'sa','Sistemas123*');
        } catch (PDOException $e) {
            echo "connection error: " . $e ;
        }

        $request = $conn->prepare("SELECT * FROM Balxpro WHERE est_esc LIKE 'PR'");
        $request->execute();
        $data = $request->fetchAll(PDO::FETCH_ASSOC);

        // var_dump($data);

        $cods = array();
        foreach( $data as $a ) {
            // echo $a['IdeComBpr'];
            array_push($cods, $a['IdeComBpr']);
        }
    
    ?>
    
    <?php 
        // $ch = curl_init();
        // curl_setopt($ch, CURLOPT_URL, "http://localhost:8000/api/clients");
        // curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        // $res = curl_exec($ch);
        // curl_close($ch);

        // print_r($res);
    ?>
</body>
</html>
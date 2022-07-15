package com.example.producer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final Logger log = LoggerFactory.getLogger(App.class.getSimpleName());

    private static String getName(){
        String[] peoples = {"Liam",	"Noah",	"Oliver",	"Elijah",	"James",	"William",	"Benjamin",	"Lucas",	"Henry",	"Theodore",	"Jack",	"Levi",	"Alexander",	"Jackson",	"Mateo",	"Daniel",	"Michael",	"Mason",	"Sebastian",	"Ethan",	"Logan",	"Owen",	"Samuel",	"Jacob",	"Asher",	"Aiden",	"John",	"Joseph",	"Wyatt",	"David",	"Leo",	"Luke",	"Julian",	"Hudson",	"Grayson",	"Matthew",	"Ezra",	"Gabriel",	"Carter",	"Isaac",	"Jayden",	"Luca",	"Anthony",	"Dylan",	"Lincoln",	"Thomas",	"Maverick",	"Elias",	"Josiah",	"Charles",	"Caleb",	"Christopher",	"Ezekiel",	"Miles",	"Jaxon",	"Isaiah",	"Andrew",	"Joshua",	"Nathan",	"Nolan",	"Adrian",	"Cameron",	"Santiago",	"Eli",	"Aaron",	"Ryan",	"Angel",	"Cooper",	"Waylon",	"Easton",	"Kai",	"Christian",	"Landon",	"Colton",	"Roman",	"Axel",	"Brooks",	"Jonathan",	"Robert",	"Jameson",	"Ian",	"Everett",	"Greyson",	"Wesley",	"Jeremiah",	"Hunter",	"Leonardo",	"Jordan",	"Jose",	"Bennett",	"Silas",	"Nicholas",	"Parker",	"Beau",	"Weston",	"Austin",	"Connor",	"Carson",	"Dominic",	"Xavier",	"Jaxson",	"Jace",	"Emmett",	"Adam",	"Declan",	"Rowan",	"Micah",	"Kayden",	"Gael",	"River",	"Ryder",	"Kingston",	"Damian",	"Sawyer",	"Luka",	"Evan",	"Vincent",	"Legend",	"Myles",	"Harrison",	"August",	"Bryson",	"Amir",	"Giovanni",	"Chase",	"Diego",	"Milo",	"Jasper",	"Walker",	"Jason",	"Brayden",	"Cole",	"Nathaniel",	"George",	"Lorenzo",	"Zion",	"Luis",	"Archer",	"Enzo",	"Jonah",	"Thiago",	"Theo",	"Ayden",	"Zachary",	"Calvin",	"Braxton",	"Ashton",	"Rhett",	"Atlas",	"Jude",	"Bentley",	"Carlos",	"Ryker",	"Adriel",	"Arthur",	"Ace",	"Tyler",	"Jayce",	"Max",	"Elliot",	"Graham",	"Kaiden",	"Maxwell",	"Juan",	"Dean",	"Matteo",	"Malachi",	"Ivan",	"Elliott",	"Jesus",	"Emiliano",	"Messiah",	"Gavin",	"Maddox",	"Camden",	"Hayden",	"Leon",	"Antonio",	"Justin",	"Tucker",	"Brandon",	"Kevin",	"Judah",	"Finn",	"King",	"Brody",	"Xander",	"Nicolas",	"Charlie",	"Arlo",	"Emmanuel",	"Barrett",	"Felix",	"Alex",	"Miguel",	"Abel",	"Alan",	"Beckett",	"Amari",	"Karter",	"Timothy",	"Abraham",	"Jesse",	"Zayden",	"Blake",	"Alejandro",	"Dawson",	"Tristan",	"Victor",	"Avery",	"Joel",	"Grant",	"Eric",	"Patrick",	"Peter",	"Richard",	"Edward",	"Andres",	"Emilio",	"Colt",	"Knox",	"Beckham",	"Adonis",	"Kyrie",	"Matias",	"Oscar",	"Lukas",	"Marcus",	"Hayes",	"Caden",	"Remington",	"Griffin",	"Nash",	"Israel",	"Steven",	"Holden",	"Rafael",	"Zane",	"Jeremy",	"Kash",	"Preston",	"Kyler",	"Jax",	"Jett",	"Kaleb",	"Riley",	"Simon",	"Phoenix",	"Javier",	"Bryce",	"Louis",	"Mark",	"Cash",	"Lennox",	"Paxton",	"Malakai",	"Paul",	"Kenneth",	"Nico",	"Kaden",	"Lane",	"Kairo",	"Maximus",	"Omar",	"Finley",	"Atticus",	"Crew",	"Brantley",	"Colin",	"Dallas",	"Walter",	"Brady",	"Callum",	"Ronan",	"Hendrix",	"Jorge",	"Tobias",	"Clayton",	"Emerson",	"Damien",	"Zayn",	"Malcolm",	"Kayson",	"Bodhi",	"Bryan",	"Aidan",	"Cohen",	"Brian",	"Cayden",	"Andre",	"Niko",	"Maximiliano",	"Zander",	"Khalil",	"Rory",	"Francisco",	"Cruz",	"Kobe",	"Reid",	"Daxton",	"Derek",	"Martin",	"Jensen",	"Karson",	"Tate",	"Muhammad",	"Jaden",	"Joaquin",	"Josue",	"Gideon",	"Dante",	"Cody",	"Bradley",	"Orion",	"Spencer",	"Angelo",	"Erick",	"Jaylen",	"Julius",	"Manuel",	"Ellis",	"Colson",	"Cairo",	"Gunner",	"Wade",	"Chance",	"Odin",	"Anderson",	"Kane",	"Raymond",	"Cristian",	"Aziel",	"Prince",	"Ezequiel",	"Jake",	"Otto",	"Eduardo",	"Rylan",	"Ali",	"Cade",	"Stephen",	"Ari",	"Kameron",	"Dakota",	"Warren",	"Ricardo",	"Killian",	"Mario",	"Romeo",	"Cyrus",	"Ismael",	"Russell",	"Tyson",	"Edwin",	"Desmond",	"Nasir",	"Remy",	"Tanner",	"Fernando",	"Hector",	"Titus",	"Lawson",	"Sean",	"Kyle",	"Elian",	"Corbin",	"Bowen",	"Wilder",	"Armani",	"Royal",	"Stetson",	"Briggs",	"Sullivan",	"Leonel",	"Callan",	"Finnegan",	"Jay",	"Zayne",	"Marshall",	"Kade",	"Travis",	"Sterling",	"Raiden",	"Sergio",	"Tatum",	"Cesar",	"Zyaire",	"Milan",	"Devin",	"Gianni",	"Kamari",	"Royce",	"Malik",	"Jared",	"Franklin",	"Clark",	"Noel",	"Marco",	"Archie",	"Apollo",	"Pablo",	"Garrett",	"Oakley",	"Memphis",	"Quinn",	"Onyx",	"Alijah",	"Baylor",	"Edgar",	"Nehemiah",	"Winston",	"Major",	"Rhys",	"Forrest",	"Jaiden",	"Reed",	"Santino",	"Troy",	"Caiden",	"Harvey",	"Collin",	"Solomon",	"Donovan",	"Damon",	"Jeffrey",	"Kason",	"Sage",	"Grady",	"Kendrick",	"Leland",	"Luciano",	"Pedro",	"Hank",	"Hugo",	"Esteban",	"Johnny",	"Kashton",	"Ronin",	"Ford",	"Mathias",	"Porter",	"Erik",	"Johnathan",	"Frank",	"Tripp",	"Casey",	"Fabian",	"Leonidas",	"Baker",	"Matthias",	"Philip",	"Jayceon",	"Kian",	"Saint",	"Ibrahim",	"Jaxton",	"Augustus",	"Callen",	"Trevor",	"Ruben",	"Adan",	"Conor",	"Dax",	"Braylen",	"Kaison",	"Francis",	"Kyson",	"Andy",	"Lucca",	"Mack",	"Peyton",	"Alexis",	"Deacon",	"Kasen",	"Kamden",	"Frederick",	"Princeton",	"Braylon",	"Wells",	"Nikolai",	"Iker",	"Bo",	"Dominick",	"Moshe",	"Cassius",	"Gregory",	"Lewis",	"Kieran",	"Isaias",	"Seth",	"Marcos",	"Omari",	"Shane",	"Keegan",	"Jase",	"Asa",	"Sonny",	"Uriel",	"Pierce",	"Jasiah",	"Eden",	"Rocco",	"Banks",	"Cannon",	"Denver",	"Zaiden",	"Roberto",	"Shawn",	"Drew",	"Emanuel",	"Kolton",	"Ayaan",	"Ares",	"Conner",	"Jalen",	"Alonzo",	"Enrique",	"Dalton",	"Moses",	"Koda",	"Bodie",	"Jamison",	"Phillip",	"Zaire",	"Jonas",	"Kylo",	"Moises",	"Shepherd",	"Allen",	"Kenzo",	"Mohamed",	"Keanu",	"Dexter",	"Conrad",	"Bruce",	"Sylas",	"Soren",	"Raphael",	"Rowen",	"Gunnar",	"Sutton",	"Quentin",	"Jaziel",	"Emmitt",	"Makai",	"Koa",	"Maximilian",	"Brixton",	"Dariel",	"Zachariah",	"Roy",	"Armando",	"Corey",	"Saul",	"Izaiah",	"Danny",	"Davis",	"Ridge",	"Yusuf",	"Ariel",	"Valentino",	"Jayson",	"Ronald",	"Albert",	"Gerardo",	"Ryland",	"Dorian",	"Drake",	"Gage",	"Rodrigo",	"Hezekiah",	"Kylan",	"Boone",	"Ledger",	"Santana",	"Jamari",	"Jamir",	"Lawrence",	"Reece",	"Kaysen",	"Shiloh",	"Arjun",	"Marcelo",	"Abram",	"Benson",	"Huxley",	"Nikolas",	"Zain",	"Kohen",	"Samson",	"Miller",	"Donald",	"Finnley",	"Kannon",	"Lucian",	"Watson",	"Keith",	"Westin",	"Tadeo",	"Sincere",	"Boston",	"Axton",	"Amos",	"Chandler",	"Leandro",	"Raul",	"Scott",	"Reign",	"Alessandro",	"Camilo",	"Derrick",	"Morgan",	"Julio",	"Clay",	"Edison",	"Jaime",	"Augustine",	"Julien",	"Zeke",	"Marvin",	"Bellamy",	"Landen",	"Dustin",	"Jamie",	"Krew",	"Kyree",	"Colter",	"Johan",	"Houston",	"Layton",	"Quincy",	"Case",	"Atreus",	"Cayson",	"Aarav",	"Darius",	"Harlan",	"Justice",	"Abdiel",	"Layne",	"Raylan",	"Arturo",	"Taylor",	"Anakin",	"Ander",	"Hamza",	"Otis",	"Azariah",	"Leonard",	"Colby",	"Duke",	"Flynn",	"Trey",	"Gustavo",	"Fletcher",	"Issac",	"Sam",	"Trenton",	"Callahan",	"Chris",	"Mohammad",	"Rayan",	"Lionel",	"Bruno",	"Jaxxon",	"Zaid",	"Brycen",	"Roland",	"Dillon",	"Lennon",	"Ambrose",	"Rio",	"Mac",	"Ahmed",	"Samir",	"Yosef",	"Tru",	"Creed",	"Tony",	"Alden",	"Aden",	"Alec",	"Carmelo",	"Dario",	"Marcel",	"Roger",	"Ty",	"Ahmad",	"Emir",	"Landyn",	"Skyler",	"Mohammed",	"Dennis",	"Kareem",	"Nixon",	"Rex",	"Uriah",	"Lee",	"Louie",	"Rayden",	"Reese",	"Alberto",	"Cason",	"Quinton",	"Kingsley",	"Chaim",	"Alfredo",	"Mauricio",	"Caspian",	"Legacy",	"Ocean",	"Ozzy",	"Briar",	"Wilson",	"Forest",	"Grey",	"Joziah",	"Salem",	"Neil",	"Remi",	"Bridger",	"Harry",	"Jefferson",	"Lachlan",	"Nelson",	"Casen",	"Salvador",	"Magnus",	"Tommy",	"Marcellus",	"Maximo",	"Jerry",	"Clyde",	"Aron",	"Keaton",	"Eliam",	"Lian",	"Trace",	"Douglas",	"Junior",	"Titan",	"Cullen",	"Cillian",	"Musa",	"Mylo",	"Hugh",	"Tomas",	"Vincenzo",	"Westley",	"Langston",	"Byron",	"Kiaan",	"Loyal",	"Orlando",	"Kyro",	"Amias",	"Amiri",	"Jimmy",	"Vicente",	"Khari",	"Brendan",	"Rey",	"Ben",	"Emery",	"Zyair",	"Bjorn",	"Evander",	"Ramon",	"Alvin",	"Ricky",	"Jagger",	"Brock",	"Dakari",	"Eddie",	"Blaze",	"Gatlin",	"Alonso",	"Curtis",	"Kylian",	"Nathanael",	"Devon",	"Wayne",	"Zakai",	"Mathew",	"Rome",	"Riggs",	"Aryan",	"Avi",	"Hassan",	"Lochlan",	"Stanley",	"Dash",	"Kaiser",	"Benicio",	"Bryant",	"Talon",	"Rohan",	"Wesson",	"Joe",	"Noe",	"Melvin",	"Vihaan",	"Zayd",	"Darren",	"Enoch",	"Mitchell",	"Jedidiah",	"Brodie",	"Castiel",	"Ira",	"Lance",	"Guillermo",	"Thatcher",	"Ermias",	"Misael",	"Jakari",	"Emory",	"Mccoy",	"Rudy",	"Thaddeus",	"Valentin",	"Yehuda",	"Bode",	"Madden",	"Kase",	"Bear",	"Boden",	"Jiraiya",	"Maurice",	"Alvaro",	"Ameer",	"Demetrius",	"Eliseo",	"Kabir",	"Kellan",	"Allan",	"Azrael",	"Calum",	"Niklaus",	"Ray",	"Damari",	"Elio",	"Jon",	"Leighton",	"Axl",	"Dane",	"Eithan",	"Eugene",	"Kenji",	"Jakob",	"Colten",	"Eliel",	"Nova",	"Santos",	"Zahir",	"Idris",	"Ishaan",	"Kole",	"Korbin",	"Seven",	"Alaric",	"Kellen",	"Bronson",	"Franco",	"Wes",	"Larry",	"Mekhi",	"Jamal",	"Dilan",	"Elisha",	"Brennan",	"Kace",	"Van",	"Felipe",	"Fisher",	"Cal",	"Dior",	"Judson",	"Alfonso",	"Deandre",	"Rocky",	"Henrik",	"Reuben",	"Anders",	"Arian",	"Damir",	"Jacoby",	"Khalid",	"Kye",	"Mustafa",	"Jadiel",	"Stefan",	"Yousef",	"Aydin",	"Jericho",	"Robin",	"Wallace",	"Alistair",	"Davion",	"Alfred",	"Ernesto",	"Kyng",	"Everest",	"Gary",	"Leroy",	"Yahir",	"Braden",	"Kelvin",	"Kristian",	"Adler",	"Avyaan",	"Brayan",	"Jones",	"Truett",	"Aries",	"Joey",	"Randy",	"Jaxx",	"Jesiah",	"Jovanni",	"Azriel",	"Brecken",	"Harley",	"Zechariah",	"Gordon",	"Jakai",	"Carl",	"Graysen",	"Kylen",	"Ayan",	"Branson",	"Crosby",	"Dominik",	"Jabari",	"Jaxtyn",	"Kristopher",	"Ulises",	"Zyon",	"Fox",	"Howard",	"Salvatore",	"Turner",	"Vance",	"Harlem",	"Jair",	"Jakobe",	"Jeremias",	"Osiris",	"Azael",	"Bowie",	"Canaan",	"Elon",	"Granger",	"Karsyn",	"Zavier",	"Cain",	"Dangelo",	"Heath",	"Yisroel",	"Gian",	"Shepard",	"Harold",	"Kamdyn",	"Rene",	"Rodney",	"Yaakov",	"Adrien",	"Kartier",	"Cassian",	"Coleson",	"Ahmir",	"Darian",	"Genesis",	"Kalel",	"Agustin",	"Wylder",	"Yadiel",	"Ephraim",	"Kody",	"Neo",	"Ignacio",	"Osman",	"Aldo",	"Abdullah",	"Cory",	"Blaine",	"Dimitri",	"Khai",	"Landry",	"Palmer",	"Benedict",	"Leif",	"Koen",	"Maxton",	"Mordechai",	"Zev",	"Atharv",	"Bishop",	"Blaise",	"Davian"};
        //String[] peoples = {"Bob","Jill","Tom","Brandon","Juan","Tomas","Juanjo","Claudia","Marta","Chaz","Diego","Pele","Raul","Arantxa","Sergio"};
        List<String> names = Arrays.asList(peoples);
        Collections.shuffle(names);
        /*for (String name : names) {
            System.out.print(name + " ");
        }*/
        int index = new Random().nextInt(names.size());
        String anynames = names.get(index);
        System.out.println("Your random name is: " + anynames + " now!");
        int index2 = new Random().nextInt(names.size());
        return anynames+System.currentTimeMillis()+index2;
    }


    private static String getColour(){
        String[] peoples = {"turquesa", "verde oliva", "verde menta", "borgoña", "lavanda", "magenta", "salmón", "cian", "beige", "rosado", "verde oscuro", "lila", "amarillo pálido", "fucsia", "mostaza", "ocre", "trullo", "malva"
        , "púrpura oscuro", "verde lima", "verde claro", "ciruela", "azul claro", "melocotón", "violeta", "tan" , "granate"};
        List<String> names = Arrays.asList(peoples);
        Collections.shuffle(names);
        /*for (String name : names) {
            System.out.print(name + " ");
        }*/
        int index = new Random().nextInt(names.size());
        String anynames = names.get(index);
        System.out.println("Your random colour is: " + anynames + " now!");
        int index2 = new Random().nextInt(names.size());
        return anynames+System.currentTimeMillis()+index2;
    }

    public static void main( String[] args )
    {
         // create Producer Properties
         Properties properties = new Properties();
         properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:19092");
         properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
         properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
 
         // create the Producer
         KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
 
 
         while(true){
 
             // create a producer record
             ProducerRecord<String, String> producerRecord =
                     new ProducerRecord<>("favourite-colour-input", getName() + ","+getColour());
 
             // send the data - asynchronous
             producer.send(producerRecord, new Callback() {
                 @Override
                 public void onCompletion(RecordMetadata metadata, Exception e) {
                     // executes every time a record is successfully sent or an exception is thrown
                     if (e == null){
                         // the record was successfully sent
                         log.info("Received new metadata. \n" +
                                 "Topic: " + metadata.topic() + "\n" +
                                 "Partition: " + metadata.partition() + "\n" +
                                 "Offset: " + metadata.offset() + "\n" +
                                 "Timestamp: " + metadata.timestamp());
                     } else {
                         log.error("Error while producing", e);
                     }
                 }
             });
 
             try {
                 Thread.sleep(20);
             } catch (InterruptedException e) {
                 e.printStackTrace();
             }
         }
 
 
         // flush data - synchronous
         //producer.flush();
 
         // flush and close producer
         //producer.close();
 
     }
 }
 